package fs_content

import (
	"io/fs"
	"log"
	"path/filepath"
	"strings"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const (
	contentInfoYaml = "content-info.yaml"
	moduleInfoYaml  = "module-info.yaml"
	unitInfoYaml    = "unit-info.yaml"
)

type learningPathInfo struct {
	Sdk         string
	ModuleNames []string `yaml:"content"`
}

type learningModuleInfo struct {
	Name       string
	Complexity string
	UnitNames  []string `yaml:"content"`
}

type learningUnitInfo struct {
	Name         string
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

func (info *learningUnitInfo) ToEntity(id string) tob.UnitContent {
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

// Build a content tree for each SDK
// Walk recursively through the learning-content dir, search for metadata files:
// content-info.yaml, module-info.yaml, unit-info.yaml
func CollectLearningTree(rootpath string) (trees []tob.ContentTree, err error) {

	var (
		treeBuilder   ContentTreeBuilder
		moduleBuilder ModuleBuilder
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
			if treeBuilder.IsInitialized() {
				trees = append(trees, treeBuilder.Build())
			}
			treeBuilder = NewContentTreeBuilder(info)

		case moduleInfoYaml:
			info := loadLearningModuleInfo(path)
			if !treeBuilder.IsInitialized() {
				log.Panicf("Module outside of sdk at %s", path)
			}
			// save previous module
			if moduleBuilder.IsInitialized() {
				treeBuilder.AddModule(moduleBuilder.Build())
			}
			id := path2Id(dirname, rootpath)
			moduleBuilder = NewModuleBuilder(id, info)
		case unitInfoYaml:
			info := loadLearningUnitInfo(path)
			if !moduleBuilder.IsInitialized() {
				log.Panicf("Unit outside of a module at %s", path)
			}
			id := path2Id(dirname, rootpath)
			moduleBuilder.AddUnit(BuildUnitContent(id, info))
		}
		return nil
	})
	if moduleBuilder.IsInitialized() {
		treeBuilder.AddModule(moduleBuilder.Build())
	}
	if treeBuilder.IsInitialized() {
		trees = append(trees, treeBuilder.Build())
	}

	return trees, err
}
