package service

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

func getSamplesPath() string {
	_, filepath, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filepath), "..", "..", "samples")
}

type Mock struct{}

// check if the interface is implemented
var _ IContent = &Mock{}

func (d *Mock) GetContentTree(_ context.Context, sdk tob.Sdk, userId *string) (ct tob.ContentTree, err error) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "content_tree.json"))
	_ = json.Unmarshal(content, &ct)
	return ct, nil
}

func (d *Mock) GetUnitContent(_ context.Context, unitId string, userId *string) (u tob.UnitContent) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "unit.json"))
	_ = json.Unmarshal(content, &u)
	return u
}
