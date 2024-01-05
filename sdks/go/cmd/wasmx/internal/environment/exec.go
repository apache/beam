package environment

import (
	"context"
	"io"
	"os/exec"
)

// Executable defines an executable on a machine.
type Executable string

func (ex Executable) which() (string, error) {
	return exec.LookPath(string(ex))
}

// Execute an Executable with args.
//
// A non-nil r or w configures its STDIN or STDOUT, respectively.
// Will return an error if Execute does not finish before a ctx's deadline, if available.
func (ex Executable) Execute(ctx context.Context, r io.Reader, w io.Writer, args ...string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	which, err := ex.which()
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, which, args...)
	if r != nil {
		cmd.Stdin = r
	}
	if w != nil {
		cmd.Stdout = w
	}

	return cmd.Run()
}
