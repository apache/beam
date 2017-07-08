package runtime

var (
	hooks       []func()
	initialized bool
)

// RegisterInit registers an Init hook. Hooks are expected to be able to
// figure out whether they apply on their own, notably if invoked in a remote
// execution environment. They are all executed regardless of the runner.
func RegisterInit(hook func()) {
	if initialized {
		panic("Init hooks have already run. Register hook during init() instead.")
	}

	hooks = append(hooks, hook)
}

// Init is the hook that all user code must call after flags processing and
// other static initialization, for now.
func Init() {
	initialized = true
	for _, hook := range hooks {
		hook()
	}
}
