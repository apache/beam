package beam

// We have some freedom to create various utilities, users can use depending on
// preferences. One point of keeping Pipeline transformation functions plain Go
// functions is that such utilities are more readily possible.

// For example, we can have an "easyio" package that selects a textio, gcsio,
// awsio, etc. transformation based on the filename schema. Such wrappers would
// look exactly like the more primitive sources/sinks, but be picked at
// pipeline construction time.

// Seq is a convenience helper to chain single-input/single-output ParDos together
// in a sequence.
func Seq(p *Pipeline, col PCollection, dofns ...interface{}) (PCollection, error) {
	cur := col
	for _, dofn := range dofns {
		next, err := ParDo(p, dofn, cur)
		if err != nil {
			return PCollection{}, err
		}
		cur = next
	}
	return cur, nil
}

func CompositeN(p *Pipeline, name string, fn func(pipeline *Pipeline) ([]PCollection, error)) ([]PCollection, error) {
	return fn(p.Composite(name))
}

func Composite0(p *Pipeline, name string, fn func(pipeline *Pipeline) error) error {
	return fn(p.Composite(name))
}

// Composite is a helper to scope a composite transform.
func Composite(p *Pipeline, name string, fn func(pipeline *Pipeline) (PCollection, error)) (PCollection, error) {
	return fn(p.Composite(name))
}
