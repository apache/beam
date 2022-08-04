package storage

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
)

type DatastoreDb struct{}

// check if the interface is implemented
var _ Iface = &DatastoreDb{}

func (d *DatastoreDb) GetContentTree(ctx context.Context, sdk string, userId *string) (ct ContentTree) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "content_tree.json"))
	_ = json.Unmarshal(content, &ct)
	return ct
}

func (d *DatastoreDb) GetUnitContent(ctx context.Context, unitId string, userId *string) (u UnitContent) {
	content, _ := ioutil.ReadFile(path.Join(getSamplesPath(), "unit.json"))
	_ = json.Unmarshal(content, &u)
	return u
}
