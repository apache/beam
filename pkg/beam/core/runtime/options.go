package runtime

import (
	"fmt"
	"sync"
)

// GlobalOptions are the global options for the active graph. Options can be
// defined at any time before execution and are re-created by the harness on
// remote execution workers. Global options should be used sparingly.
var GlobalOptions = &Options{opt: make(map[string]string)}

// Options are untyped options.
type Options struct {
	opt map[string]string
	ro  bool
	mu  sync.Mutex
}

// RawOptions represents exported options as JSON-serializable data.
// Exact representation is subject to change.
type RawOptions struct {
	Options map[string]string `json:"options"`
}

// Import imports the options from previously exported data and makes the
// options read-only. It panics if import is called twice.
func (o *Options) Import(opt RawOptions) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ro {
		panic("import failed: options read-only")
	}
	o.ro = true
	o.opt = copyMap(opt.Options)
}

// Get returns the value of the key. If the key has not been set, it returns "".
func (o *Options) Get(key string) string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.opt[key]
}

// Set defines the value of the given key. If the key is already defined, it
// panics.
func (o *Options) Set(key, value string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ro {
		return // ignore silently to allow init-time set of options
	}
	if _, ok := o.opt[key]; ok {
		panic(fmt.Sprintf("option %v already defined", key))
	}
	o.opt[key] = value
}

// Export returns a JSON-serializable copy of the options.
func (o *Options) Export() RawOptions {
	o.mu.Lock()
	defer o.mu.Unlock()

	return RawOptions{Options: copyMap(o.opt)}
}

func copyMap(m map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}
