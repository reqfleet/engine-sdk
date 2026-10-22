// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"os"
	"path"
)

const (
	FILEMODE = 0700
)

type (
	AgentDir struct {
		dir          string
		testFilesDir string
	}
	AgentDirectory       string
	TestFilesDirectory   string
	ResultFilesDirectory string
	ConfFilesDirectory   string
)

func NewAgentDirHandler(dir string) AgentDir {
	if dir == "" {
		dir = os.Getenv("AGENT_ROOT")
	}
	return AgentDir{dir: dir}
}

func (af AgentDir) Dir() AgentDirectory {
	return AgentDirectory(af.dir)
}

func (af AgentDir) TestFilesDir() TestFilesDirectory {
	if af.testFilesDir != "" {
		return TestFilesDirectory(af.testFilesDir)
	}
	return TestFilesDirectory(path.Join("", "/test-data"))
}

func (af AgentDir) ConfFilesDir() ConfFilesDirectory {
	return ConfFilesDirectory(path.Join(af.dir, "test-conf"))
}

func (af AgentDir) ResultFilesDir() ResultFilesDirectory {
	return ResultFilesDirectory(path.Join(af.dir, "test-result"))
}

func (tf TestFilesDirectory) reset() error {
	files, err := os.ReadDir(string(tf))
	if err != nil {
		return err
	}
	for _, file := range files {
		f := path.Join(string(tf), file.Name())
		if err := os.RemoveAll(f); err != nil {
			return err
		}
	}
	return nil
}

func join(parent string, subdirs ...string) string {
	full := make([]string, len(subdirs)+1)
	full[0] = parent
	for i, item := range subdirs {
		full[i+1] = item
	}
	return path.Join(full...)
}

func (af AgentDirectory) Filepath(sub ...string) string {
	return join(string(af), sub...)
}

func (cf ConfFilesDirectory) Filepath(sub ...string) string {
	return join(string(cf), sub...)
}

func (tf TestFilesDirectory) Filepath(sub ...string) string {
	return join(string(tf), sub...)
}

func (rf ResultFilesDirectory) ResultFile(sub ...string) string {
	return join(string(rf), sub...)
}
