package dataflow

import (
	"context"
	"flag"

	"beam"
)

// GOOD: Run doesn't have to be a method on the pipeline. Each runner could simply have
// a function to execute a job: dataflow.Execute(p, options). A wrapper that knows about
// all runners would then pick one from a flag, say.

// GOOD: Would remove some of the job-level config from the runtime config. For example,
// whether autoscaling enabled is an option solely meaningful to the dataflow runner at
// submission time? Java has the superset of everything, which is less then awesome.

// NEUTRAL: We don't validate options until execution time. Java does syntax up front, but
// otherwise can't validate completeness until the runner is selected. Given that the transforms
// are determined at pipeline construction time, they can't be validated earlier (without
// lots of semantically-empty defaults that partly defeats the value of validation anyway).

var (
	project = flag.String("project", "", "GCP project to use for job submission.")
)

// TODO: we need a way to bind values at submission time to config options at runtime.
// Should we automatically propagate values to runtime? Seems wrong.

func Execute(ctx context.Context, p *beam.Pipeline /* more Dataflow options */) error {
	p.Build()
	// TODO: translate and submit.
	return nil
}
