package main

import (
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
	_, copyErr := io.Copy(output, contextReader{ctx: ctx, reader: source})
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
		Args:    []string{"--config", preparedConfigPath(run)},
	}, nil
}
