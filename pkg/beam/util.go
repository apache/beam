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
	for _, dofn := range dofns {
		next, err := ParDo1(p, dofn, col)
		if err != nil {
			return PCollection{}, err
		}
		col = next
	}
	return col, nil
}
