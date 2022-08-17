package storage

import tob "beam.apache.org/learning/tour-of-beam/backend/internal"

func (lm *TbLearningUnit) ToEntity() tob.UnitContent {
	return tob.UnitContent{Unit: tob.Unit{Id: lm.Id, Name: lm.Name}}
}

func (lm *TbLearningModule) ToEntity() (mod tob.Module) {
	mod.Id = lm.Id
	mod.Name = lm.Name
	mod.Units = make([]tob.UnitContent, len(lm.Units))
	for i, tbUnit := range lm.Units {
		mod.Units[i] = tbUnit.ToEntity()
	}
	return mod
}

func (lp *TbLearningPath) ToEntity() (tree tob.ContentTree) {
	tree.Sdk = tob.FromString(lp.Sdk)
	tree.Modules = make([]tob.Module, len(lp.Modules))
	for i, tbMod := range lp.Modules {
		tree.Modules[i] = tbMod.ToEntity()
	}

	return tree
}
