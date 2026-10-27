package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/reqfleet/engine-sdk/agent"
)

func main() {
	if err := agent.MakeAgentServer(newAgentServerOptions()).Run(); err != nil {
		log.Fatal(err)
	}
}

func newAgentServerOptions() agent.AgentServerOptions {
	return agent.AgentServerOptions{
		TestFileName: "original.curl",
		BeforeStart:  beforeStart,
		BuildCommand: buildCommand,
	}
}

type settings struct {
	concurrency     int
	durationSeconds int
	planName        string
}

func decodeSettings(raw json.RawMessage) (settings, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return settings{}, fmt.Errorf("settings must be a JSON object: %w", err)
	}
	if fields == nil {
		return settings{}, errors.New("settings must be a JSON object")
	}
	var values [3]string
	for i, key := range [...]string{"concurrency", "duration", "plan_name"} {
		value, ok := fields[key]
		if !ok && key == "plan_name" {
			continue
		}
		if !ok || len(value) == 0 || value[0] != '"' {
			return settings{}, fmt.Errorf("settings %s must be a string", key)
		}
		if err := json.Unmarshal(value, &values[i]); err != nil {
			return settings{}, fmt.Errorf("settings %s: %w", key, err)
		}
	}
	concurrency, err := positiveInteger("concurrency", values[0])
	if err != nil {
		return settings{}, err
	}
	duration, err := positiveInteger("duration", values[1])
	if err != nil {
		return settings{}, err
	}
	if duration > math.MaxInt/60 {
		return settings{}, errors.New("settings duration overflows seconds conversion")
	}
	name := values[2]
	if strings.ContainsAny(name, "\r\n\x00") {
		return settings{}, errors.New("settings plan_name cannot contain CR, LF, or NUL in an HTTP header")
	}
	return settings{concurrency: concurrency, durationSeconds: duration * 60, planName: name}, nil
}

func positiveInteger(field, value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("settings %s must be a positive integer: %w", field, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("settings %s must be a positive integer", field)
	}
	return n, nil
}

// BeforeStart runs after the SDK stages artifacts and before it constructs or
// launches the workload. Only run-local generated output belongs to this hook.
func beforeStart(ctx context.Context, run agent.RunContext) (resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("prepare curl plan: %w", resultErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	assigned, err := decodeSettings(run.Settings)
	if err != nil {
		return err
	}
	source, err := os.Open(run.TestFile)
	if err != nil {
		return fmt.Errorf("open original %q: %w", run.TestFile, err)
	}
	output, err := os.CreateTemp(run.Workspace.Temp, ".prepared-*.curl")
	if err != nil {
		return errors.Join(fmt.Errorf("create prepared config: %w", err), source.Close())
	}
	closed, published := false, false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, output.Close())
		}
		cleanupPath := output.Name()
		if published {
			if resultErr == nil {
				return
			}
			cleanupPath = preparedConfigPath(run)
		}
		if err := os.Remove(cleanupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove incomplete config %q: %w", cleanupPath, err))
		}
	}()
	copyErr := copySingleOperationConfig(ctx, output, source)
	if err := errors.Join(copyErr, source.Close()); err != nil {
		return fmt.Errorf("copy original %q: %w", run.TestFile, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// curl's last occurrence wins for these scalar options. A leading newline
	// also separates our options from an original without a trailing newline.
	_, err = fmt.Fprintf(output, "\nparallel\nparallel-max = %d\nmax-time = %d\nheader = %s\n",
		assigned.concurrency, assigned.durationSeconds, quoteCurl("X-Plan-Name: "+assigned.planName))
	if err != nil {
		return fmt.Errorf("append managed curl options: %w", err)
	}
	closed = true
	if err := output.Close(); err != nil {
		return fmt.Errorf("close prepared config: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(output.Name(), preparedConfigPath(run)); err != nil {
		return fmt.Errorf("publish prepared config: %w", err)
	}
	published = true
	return ctx.Err()
}

func copySingleOperationConfig(ctx context.Context, output io.Writer, source io.Reader) error {
	// Tee the input so validation never normalizes whitespace or line endings.
	scanner := bufio.NewScanner(io.TeeReader(contextReader{ctx: ctx, reader: source}, output))
	// Match the 100 KiB config-line limit of curl in the Debian runtime image.
	scanner.Buffer(make([]byte, 4096), 100*1024)
	for line := 1; scanner.Scan(); line++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkCurlOption(scanner.Bytes()); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return scanner.Err()
}

func checkCurlOption(line []byte) error {
	if bytes.IndexByte(line, 0) >= 0 {
		return errors.New("curl config must not contain NUL bytes")
	}
	line = bytes.TrimLeft(line, " \t\r\v\f")
	if len(line) == 0 || line[0] == '#' || line[0] == '/' || line[0] == '*' {
		return nil
	}
	short := line[0] == '-' && !bytes.HasPrefix(line, []byte("--"))
	separators := " \t\r\v\f=:"
	if short {
		separators = " \t\r\v\f"
	}
	if end := bytes.IndexAny(line, separators); end >= 0 {
		line = line[:end]
	}
	if short {
		for _, option := range line[1:] {
			switch option {
			case ':':
				return errors.New("curl operation separators (next) are not supported")
			case 'K':
				return errors.New("curl config includes are not supported")
			}
			// Only argument-free short options can precede another option.
			// Stop before attached values such as -HX-Name:next.
			if !strings.ContainsRune("012346aBfgGhiIjJklLMnNOpqRsSvVZ#", rune(option)) {
				break
			}
		}
		return nil
	}
	option := bytes.TrimPrefix(line, []byte("--"))
	// curl also accepts case-insensitive, unambiguous long-option prefixes.
	if bytes.EqualFold(option, []byte("next")) || bytes.EqualFold(option, []byte("nex")) {
		return errors.New("curl operation separators (next) are not supported")
	}
	if bytes.EqualFold(option, []byte("config")) || bytes.EqualFold(option, []byte("confi")) ||
		bytes.EqualFold(option, []byte("conf")) {
		return errors.New("curl config includes are not supported")
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// curl config quoting is not Go or shell quoting: only curl's documented
// backslash escapes are used, with Unicode left intact.
func quoteCurl(value string) string {
	escaped := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\t", "\\t", "\v", "\\v").Replace(value)
	return "\"" + escaped + "\""
}

func preparedConfigPath(run agent.RunContext) string {
	return filepath.Join(run.Workspace.Temp, "prepared.curl")
}

func buildCommand(ctx context.Context, run agent.RunContext) (agent.Command, error) {
	if err := ctx.Err(); err != nil {
		return agent.Command{}, err
	}
	return agent.Command{
		Command: "curl",
		Args:    []string{"--disable", "--config", preparedConfigPath(run)},
	}, nil
}
