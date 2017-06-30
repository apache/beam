package window

import "fmt"

// Window defines the types of windowing used in a pipeline and contains
// the data and code to support executing a windowing strategy.
type Window struct {
	k Kind
	// TODO: pointer to windowing function
	// TODO: other fields
}

// Kind is the semantic type of window.
type Kind string

const (
	// GlobalWindow is the default window into which all elements are placed.
	GlobalWindow Kind = "GW"
)

// NewGlobalWindow returns the default window to be used for a collection.
func NewGlobalWindow() *Window {
	return &Window{k: GlobalWindow}
}
func (w *Window) String() string {
	return string(w.k)
}

// Kind returns the kind of the window.
func (w *Window) Kind() Kind {
	return w.k
}

// Equals returns true iff the windows have the same kind and underlying behavior.
// Built-in window types (such as global window) are only equal to the same
// instances of the window. A user-defined window that happens to match a
// built-in will not match on Equals().
func (w *Window) Equals(o *Window) bool {
	switch w.Kind() {
	case GlobalWindow:
		return o.Kind() == w.Kind()
	default:
		panic(fmt.Sprintf("unknown window type: %v", w))
		// TODO(wcn): implement other window types
	}
}

// TODO: Sessions, FixedWindows, etc.

// CustomWindow is the base-case windowing that relies on a user
// specified function for windowing behavior
// var CustomWindow WindowKind

// TODO(wcn): establish the other window types when we are
// ready to support them.
