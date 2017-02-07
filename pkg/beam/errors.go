package beam

import (
	"fmt"
	"runtime"
)

// TODO(herohde) 2/6/2017: placeholder.

// ErrorList is an accumulator for pipeline-construction errors. We place them
// here -- as opposed to the pcollection -- to handle sinks at al better.
type ErrorList struct {
	errs []error
}

func (a *ErrorList) Add(err error) error {
	a.errs = append(a.errs, err)
	return err
}

func (a *ErrorList) Errors() []error {
	return a.errs
}

// TODO(herohde) 2/6/2017: perhaps we should capture the call stack?

// Errorf is a fmt.Errorf wrapper that prefixes source location.
func Errorf(depth int, msg string, args ...interface{}) error {
	if pc, file, line, ok := runtime.Caller(depth); ok {
		name := runtime.FuncForPC(pc).Name()

		msg = "%v (%v:%v): " + msg
		args = append([]interface{}{name, file, line}, args...)
	}
	return fmt.Errorf(msg, args...)
}
