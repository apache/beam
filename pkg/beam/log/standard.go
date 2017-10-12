package log

import (
	"context"
	stdlog "log"
)

// Standard is a wrapper over the standard Go logger.
type Standard struct {
	// Level is the severity cutoff for log messages. By default all
	// messages are logged.
	Level Severity
}

// Log logs the message to the standard Go logger. For Panic, it does not
// perform the os.Exit(1) call, but defers to the log wrapper.
func (s *Standard) Log(ctx context.Context, sev Severity, calldepth int, msg string) {
	if sev < s.Level {
		return
	}
	stdlog.Output(calldepth+1, msg)
}
