// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import "os/exec"

type Command struct {
	Command string
	Args    []string
	Dir     string
	Env     []string
}

func (c Command) ToExec() *exec.Cmd {
	cmd := exec.Command(c.Command, c.Args...)
	cmd.Dir = c.Dir
	cmd.Env = c.Env
	return cmd
}
