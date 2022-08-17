package fs_content

import (
	"io/fs"
	"log"
	"path/filepath"
	"sort"
	"strings"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const (
	contentInfoYaml = "content-info.yaml"
	moduleInfoYaml  = "module-info.yaml"
	unitInfoYaml    = "unit-info.yaml"
)

type learningPathInfo struct {
	Sdk     string
	Modules []string `yaml:"content"`
}

type learningModuleInfo struct {
	Name       string
	Complexity string
	Units      []string `yaml:"content"`
}

type learningUnitInfo struct {
	Name         string
	Complexity   string
	TaskName     string
	SolutionName string
}

// Get Unit/ModuleId from its VCS path
// - convert '/' and '.' to '#' and '*': this is a datastore key
// - strip the common prefix with parent
func path2Id(path, parent string) string {
	path = strings.TrimPrefix(path, parent)
	path = strings.Trim(path, "/")
	path = strings.ReplaceAll(path, ".", "*")
	path = strings.ReplaceAll(path, "/", "#")
	return path
}

func (info *learningPathInfo) ToEntity() tob.ContentTree {
	sdk := tob.FromString(info.Sdk)
	if sdk == tob.SDK_UNDEFINED {
		log.Panicf("Undefined sdk %s", info.Sdk)
	}
	return tob.ContentTree{Sdk: sdk}
}

func (info *learningModuleInfo) ToEntity(id string) tob.Module {
	return tob.Module{Id: id, Name: info.Name}
}

func (info *learningModuleInfo) finalizeModule(mod *tob.Module) {
	if len(mod.Units) != len(info.Units) {
		log.Panicf("Module %s units number mismatch (expected %s)", mod.Name, len(info.Units))
	}

	sort.Sort()
}

func (info *learningUnitInfo) ToEntity(id string) tob.UnitContent {
	return tob.UnitContent{
		Unit: tob.Unit{
			Id:   id,
			Name: info.Name,
		},
		// TODO: description, hints

		Complexity:   info.Complexity,
		TaskName:     info.TaskName,
		SolutionName: info.SolutionName,
	}
}

// Build a content tree for each SDK
// Walk recursively through the learning-content dir, search for metadata files:
// content-info.yaml, module-info.yaml, unit-info.yaml
func CollectLearningTree(rootpath string) (trees []tob.ContentTree, err error) {

	var (
		currentTree   *tob.ContentTree
		currentModule *tob.Module
	)

	err = filepath.WalkDir(rootpath, func(path string, d fs.DirEntry, err error) error {
		// terminate walk on any error
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		dirname, fname := filepath.Split(path)
		switch fname {
		case contentInfoYaml:
			info := loadLearningPathInfo(path)
			trees = append(trees, info.ToEntity())
			if err := info.finalizeTree(currentTree); err != nil {
				return err
			}
			currentTree = &trees[len(trees)-1]
		case moduleInfoYaml:
			info := loadLearningModuleInfo(path)
			if currentTree == nil {
				log.Panicf("Module outside of sdk at %s", path)
			}
			id := path2Id(dirname, rootpath)
			mods := &currentTree.Modules
			*mods = append(*mods, info.ToEntity(id))
			if err := finalizeModule(currentModule); err != nil {
				return err
			}
			currentModule = &(*mods)[len(*mods)-1]
		case unitInfoYaml:
			info := loadLearningUnitInfo(path)
			if currentModule == nil {
				log.Panicf("Unit outside of a module at %s", path)
			}
			id := path2Id(dirname, rootpath)
			units := &currentModule.Units
			*units = append(*units, info.ToEntity(id))
		}
		return nil
	})

	return trees, err
}
