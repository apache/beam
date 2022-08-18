package fs_content

import (
	"io/fs"
	"log"
	"path/filepath"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const (
	contentInfoYaml = "content-info.yaml"
	moduleInfoYaml  = "module-info.yaml"
	unitInfoYaml    = "unit-info.yaml"
)

type learningPathInfo struct {
	Sdk        string
	ModuleDirs []string `yaml:"content"`
}

type learningModuleInfo struct {
	Name       string
	Complexity string
	UnitDirs   []string `yaml:"content"`
}

type learningUnitInfo struct {
	Id           string
	Name         string
	TaskName     string
	SolutionName string

	dirName string
}

// Build a content tree for each SDK
// Walk recursively through the learning-content dir, search for metadata files:
// content-info.yaml, module-info.yaml, unit-info.yaml
func CollectLearningTree(rootpath string) (trees []tob.ContentTree, err error) {

	var (
		treeBuilder    ContentTreeBuilder
		moduleBuilder  ModuleBuilder
		dirName, fName string
	)

	err = filepath.WalkDir(rootpath, func(path string, d fs.DirEntry, err error) error {
		// terminate walk on any error
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		dirName, fName = filepath.Split(path)
		switch fName {
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
				treeBuilder.AddModule(moduleBuilder.Build(), dirName)
			}
			moduleBuilder = NewModuleBuilder(info)
		case unitInfoYaml:
			info := loadLearningUnitInfo(path)
			if !moduleBuilder.IsInitialized() {
				log.Panicf("Unit outside of a module at %s", path)
			}
			moduleBuilder.AddUnit(BuildUnitContent(info), dirName)
		}
		return nil
	})
	// finalize last tree & module
	if treeBuilder.IsInitialized() {
		if moduleBuilder.IsInitialized() {
			treeBuilder.AddModule(moduleBuilder.Build(), dirName)
		}
		trees = append(trees, treeBuilder.Build())
	}

	return trees, err
}
