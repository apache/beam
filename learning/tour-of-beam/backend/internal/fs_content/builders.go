package fs_content

import (
	"log"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

type ContentTreeBuilder struct {
	initialized bool
	info        learningPathInfo
	modules     []tob.Module
}

func NewContentTreeBuilder(info learningPathInfo) ContentTreeBuilder {
	return ContentTreeBuilder{initialized: true, info: info}
}

func (b *ContentTreeBuilder) IsInitialized() bool {
	return b.initialized
}

func (b *ContentTreeBuilder) AddModule(mod tob.Module) {
	b.modules = append(b.modules, mod)
}

// sort learning_path's modules in the order corresponding to content-info.yaml/content
func (b *ContentTreeBuilder) sortModules() {
	if len(b.modules) != len(b.info.ModuleNames) {
		log.Panicf("Tree %v modules number mismatch (expected %v)", b.info.Sdk, len(b.info.ModuleNames))
	}

	newModules := make([]tob.Module, len(b.modules))
	for i, moduleName := range b.info.ModuleNames {
		for _, mod := range b.modules {
			if mod.Name == moduleName {
				newModules[i] = mod
			}
		}
	}
	b.modules = newModules
}

func (b *ContentTreeBuilder) Build() tob.ContentTree {
	sdk := tob.FromString(b.info.Sdk)
	if sdk == tob.SDK_UNDEFINED {
		log.Panicf("Undefined sdk %s", b.info.Sdk)
	}
	b.sortModules()
	return tob.ContentTree{Sdk: sdk, Modules: b.modules}
}

type ModuleBuilder struct {
	id          string
	initialized bool
	info        learningModuleInfo
	units       []tob.UnitContent
}

func NewModuleBuilder(id string, info learningModuleInfo) ModuleBuilder {
	return ModuleBuilder{id: id, initialized: true, info: info}
}

func (b *ModuleBuilder) IsInitialized() bool {
	return b.initialized
}

func (b *ModuleBuilder) AddUnit(unit tob.UnitContent) {
	b.units = append(b.units, unit)
}

// sort module's units in the order corresponding to module-info.yaml/content
func (b *ModuleBuilder) sortUnits() {
	if len(b.units) != len(b.info.UnitNames) {
		log.Panicf("Module %v units number mismatch (expected %v)", b.info.Name, len(b.info.UnitNames))
	}

	newUnits := make([]tob.UnitContent, len(b.units))
	for i, unitName := range b.info.UnitNames {
		for _, unit := range b.units {
			if unit.Name == unitName {
				newUnits[i] = unit
			}
		}
	}
	b.units = newUnits
}

func (b *ModuleBuilder) Build() tob.Module {
	b.sortUnits()
	return tob.Module{
		Id:         b.id,
		Name:       b.info.Name,
		Complexity: b.info.Complexity,
		Units:      b.units,
	}
}

// Make a Builder
func BuildUnitContent(id string, info learningUnitInfo) tob.UnitContent {
	return tob.UnitContent{
		Unit: tob.Unit{
			Id:   id,
			Name: info.Name,
		},
		// TODO: description, hints

		TaskName:     info.TaskName,
		SolutionName: info.SolutionName,
	}
}
