package storage

import (
	"encoding/json"
	"io/ioutil"
)

type Mock struct{}

func (d *Mock) GetContentTree(sdk string, userId *string) ContentTree {
	content, _ := ioutil.ReadFile("samples/content_tree.json")

	var ct ContentTree
	_ = json.Unmarshal(content, &ct)
	return ct
}
