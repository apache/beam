package beam

import (
	"context"
	"fmt"

	"log"
)

// TODO(herohde) 7/6/2017: do we want to make the selected runner visible to
// transformations? That would allow runner-dependent operations or
// verification, but require that it is stored in Init and used for Run.

var (
	hooks       []func()
	initialized bool

	runners = make(map[string]func(ctx context.Context, p *Pipeline) error)
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

// RegisterRunner associates the name with the supplied runner, making it available
// to execute a pipeline via Run.
func RegisterRunner(name string, fn func(ctx context.Context, p *Pipeline) error) {
	if _, ok := runners[name]; ok {
		panic(fmt.Sprintf("runner %v already defined", name))
	}
	runners[name] = fn
}

// Init is the hook that all user code must call after flags processing and
// other static initialization, for now.
func Init() {
	initialized = true
	for _, hook := range hooks {
		hook()
	}
}

// Run executes the pipeline using the selected registred runner. It is customary
// to define a "runner" with no default as a flag to let users control runner
// selection.
func Run(ctx context.Context, runner string, p *Pipeline) error {
	fn, ok := runners[runner]
	if !ok {
		log.Fatalf("Runner %v not registed. Forgot to _ import it?", runner)
	}
	return fn(ctx, p)
}
