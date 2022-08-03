package storage

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
)

func getSamplesPath() string {
	_, filepath, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filepath), "..", "..", "samples")
}

type Mock struct{}

// check if the interface is implemented
var _ Iface = &Mock{}

func (d *Mock) GetContentTree(sdk string, userId *string) (ct ContentTree) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "content_tree.json"))
	_ = json.Unmarshal(content, &ct)
	return ct
}

func (d *Mock) GetUnitContent(unitId string, userId *string) (u UnitContent) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "unit.json"))
	_ = json.Unmarshal(content, &u)
	return u
}
