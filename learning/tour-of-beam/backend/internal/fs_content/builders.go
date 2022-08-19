package fs_content

import (
	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

type UnitBuilder struct {
	tob.Unit
}

func NewUnitBuilder(info learningUnitInfo) UnitBuilder {
	return UnitBuilder{tob.Unit{
		Id:           info.Id,
		Name:         info.Name,
		TaskName:     info.TaskName,
		SolutionName: info.SolutionName,
	}}
}

func (b *UnitBuilder) AddDescription(d []byte) {
	b.Description = d
}

func (b *UnitBuilder) AddHint(h []byte) {
	b.Hints = append(b.Hints, h)
}

func (b *UnitBuilder) Build() tob.Unit {
	return b.Unit
}
