package exec

import (
	"context"
)

type UnitID int

// Unit represents a processing unit.
type Unit interface {
	ID() UnitID
	Up(ctx context.Context) error
	Down(ctx context.Context) error

	// Status()
	// Error() error
}

// Root represents a root processing unit. It contains its processing
// continuation, notably other other nodes.
type Root interface {
	Unit

	// Process processes the entire source, notably emitting elements to
	// downstream nodes.
	Process(ctx context.Context) error
}

// Node represents an single-bundle processing unit. Each node contains
// its processing continuation, notably other nodes.
type Node interface {
	Unit

	// Call processes a single element. If GBK or CoGBK result, the values
	// are populated. Otherwise, they're empty.
	ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error
}

// GenID is a UnitID generator.
type GenID struct {
	last int
}

func (g *GenID) New() UnitID {
	g.last++
	return UnitID(g.last)
}

func Up(ctx context.Context, list ...Node) error {
	for _, out := range list {
		if err := out.Up(ctx); err != nil {
			return err
		}
	}
	return nil
}

func Down(ctx context.Context, list ...Node) error {
	for _, out := range list {
		if err := out.Down(ctx); err != nil {
			return err
		}
	}
	return nil
}

func IDs(list ...Node) []UnitID {
	var ret []UnitID
	for _, n := range list {
		ret = append(ret, n.ID())
	}
	return ret
}
