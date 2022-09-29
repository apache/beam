// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fs_content

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const (
	contentInfoYaml = "content-info.yaml"
	moduleInfoYaml  = "module-info.yaml"
	groupInfoYaml   = "group-info.yaml"
	unitInfoYaml    = "unit-info.yaml"

	descriptionMd = "description.md"
	hintMdRegexp  = "hint[0-9]*.md"
)

type learningPathInfo struct {
	Sdk     string
	Content []string `yaml:"content"`
}

type learningModuleInfo struct {
	Id         string
	Name       string
	Complexity string
	Content    []string `yaml:"content"`
}

type learningGroupInfo struct {
	Name    string
	Content []string `yaml:"content"`
}

type learningUnitInfo struct {
	Id           string
	Name         string
	TaskName     string
	SolutionName string
}

// Watch for duplicate ids. Not thread-safe!
type idsWatcher struct {
	ids map[string]struct{}
}

func (w *idsWatcher) CheckId(id string) {
	if _, exists := w.ids[id]; exists {
		log.Fatalf("Duplicate id: %v", id)
	}
	w.ids[id] = struct{}{}
}

func NewIdsWatcher() idsWatcher {
	return idsWatcher{make(map[string]struct{})}
}

func collectUnit(infopath string, ids_watcher *idsWatcher) (unit *tob.Unit, err error) {
	info := loadLearningUnitInfo(infopath)
	log.Printf("Found Unit %v metadata at %v\n", info.Id, infopath)
	ids_watcher.CheckId(info.Id)
	builder := NewUnitBuilder(info)

	rootpath := filepath.Join(infopath, "..")
	err = filepath.WalkDir(rootpath,
		func(path string, d fs.DirEntry, err error) error {
			switch {
			// skip nested dirs
			case path > rootpath && d.IsDir():
				return filepath.SkipDir

			case d.Name() == descriptionMd:
				content, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}
				builder.AddDescription(string(content))

			// Here we rely on that WalkDir entries are lexically sorted
			case regexp.MustCompile(hintMdRegexp).MatchString(d.Name()):
				content, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}
				builder.AddHint(string(content))
			}
			return nil
		})

	return builder.Build(), err
}

func collectGroup(infopath string, ids_watcher *idsWatcher) (*tob.Group, error) {
	info := loadLearningGroupInfo(infopath)
	log.Printf("Found Group %v metadata at %v\n", info.Name, infopath)
	group := tob.Group{Title: info.Name}
	for _, item := range info.Content {
		node, err := collectNode(filepath.Join(infopath, "..", item), ids_watcher)
		if err != nil {
			return &group, err
		}
		group.Nodes = append(group.Nodes, node)
	}

	return &group, nil
}

// Collect node which is either a unit or a group.
func collectNode(rootpath string, ids_watcher *idsWatcher) (node tob.Node, err error) {
	files, err := os.ReadDir(rootpath)
	if err != nil {
		return node, err
	}
	for _, f := range files {
		switch f.Name() {
		case unitInfoYaml:
			node.Type = tob.NODE_UNIT
			node.Unit, err = collectUnit(filepath.Join(rootpath, unitInfoYaml), ids_watcher)
		case groupInfoYaml:
			node.Type = tob.NODE_GROUP
			node.Group, err = collectGroup(filepath.Join(rootpath, groupInfoYaml), ids_watcher)
		}
	}
	if node.Type == tob.NODE_UNDEFINED {
		return node, fmt.Errorf("node undefined at %v", rootpath)
	}
	return node, err
}

func collectModule(infopath string, ids_watcher *idsWatcher) (tob.Module, error) {
	info := loadLearningModuleInfo(infopath)
	log.Printf("Found Module %v metadata at %v\n", info.Id, infopath)
	ids_watcher.CheckId(info.Id)
	module := tob.Module{Id: info.Id, Title: info.Name, Complexity: info.Complexity}
	for _, item := range info.Content {
		node, err := collectNode(filepath.Join(infopath, "..", item), ids_watcher)
		if err != nil {
			return tob.Module{}, err
		}
		module.Nodes = append(module.Nodes, node)
	}

	return module, nil
}

func collectSdk(infopath string) (tree tob.ContentTree, err error) {
	ids_watcher := NewIdsWatcher()

	info := loadLearningPathInfo(infopath)
	tree.Sdk = tob.ParseSdk(info.Sdk)
	if tree.Sdk == tob.SDK_UNDEFINED {
		return tree, fmt.Errorf("unknown SDK at %v", infopath)
	}
	log.Printf("Found Sdk %v metadata at %v\n", info.Sdk, infopath)
	for _, item := range info.Content {
		mod, err := collectModule(filepath.Join(infopath, "..", item, moduleInfoYaml), &ids_watcher)
		if err != nil {
			return tree, err
		}
		tree.Modules = append(tree.Modules, mod)
	}

	return tree, nil
}

// Build a content tree for each SDK
// Walk recursively through the learning-content dir, search for metadata files:
// content-info.yaml, module-info.yaml, unit-info.yaml.
func CollectLearningTree(rootpath string) (trees []tob.ContentTree, err error) {
	err = filepath.WalkDir(rootpath, func(path string, d fs.DirEntry, err error) error {
		// terminate walk on any error
		if err != nil {
			return err
		}
		if d.Name() == contentInfoYaml {
			tree, err := collectSdk(path)
			if err != nil {
				return err
			}
			trees = append(trees, tree)
			// don't walk into SDK subtree (already done by collectSdk)
			return filepath.SkipDir
		}
		return nil
	})

	return trees, err
}
