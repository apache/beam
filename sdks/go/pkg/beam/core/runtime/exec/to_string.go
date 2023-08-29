package exec

import (
	"context"
	"fmt"
)

type ToString struct {
	// UID is the unit identifier.
	UID UnitID
	// Out is the output node.
	Out Node
}

func (m *ToString) ID() UnitID {
	return m.UID
}

func (m *ToString) Up(ctx context.Context) error {
	return nil
}

func (m *ToString) StartBundle(ctx context.Context, id string,
	data DataContext) error {
	return m.Out.StartBundle(ctx, id, data)
}

func (m *ToString) ProcessElement(ctx context.Context, elm *FullValue,
	values ...ReStream) error {
	ret := FullValue{
		Windows:   elm.Windows,
		Elm:       elm.Elm,
		Elm2:      fmt.Sprintf("%v", elm.Elm2),
		Timestamp: elm.Timestamp,
	}

	return m.Out.ProcessElement(ctx, &ret, values...)
}

func (m *ToString) FinishBundle(ctx context.Context) error {
	return m.Out.FinishBundle(ctx)
}

func (m *ToString) Down(ctx context.Context) error {
	return nil
}

func (m *ToString) String() string {
	return fmt.Sprintf("ToStringFn. Out:%v", m.Out)
}
