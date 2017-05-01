package graph

// Scope is a syntactic Scope, such as arising from a composite PTransform. It
// has no semantic meaning at execution time. Used by monitoring.
type Scope struct {
	id int

	// Label is the human-visible label for this scope.
	Label string
	// Parent is the parent scope, if nested.
	Parent *Scope
}

// ID returns the graph-local identifier for the scope.
func (s *Scope) ID() int {
	return s.id
}

func (s *Scope) String() string {
	if s.Parent == nil {
		return s.Label
	}
	return s.Parent.String() + "/" + s.Label
}
