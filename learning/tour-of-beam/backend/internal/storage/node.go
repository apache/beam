// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"cloud.google.com/go/datastore"
)

// Combination of Unit and Group
type TbNode struct {
	Key      *datastore.Key `datastore:"__key__"`
	NodeType tob.NodeType   `datastore:"node_type"`

	unit  *TbLearningUnit
	group *TbLearningGroup
}

// two-step load of tb_node entity,
// 1. Determine node type
// 2. Load it as a unit or a group
func (node *TbNode) Load(ps []datastore.Property) error {
	// Get the discriminator field value
	err := datastore.LoadStruct(node, ps)
	if err != nil {
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			return err
		}
	}

	if node.NodeType == tob.NODE_GROUP {
		node.group = new(TbLearningGroup)
		err = datastore.LoadStruct(node.group, ps)
	} else {
		node.unit = new(TbLearningUnit)
		err = datastore.LoadStruct(node.unit, ps)
	}

	return err
}

func (node *TbNode) Save() ([]datastore.Property, error) {
	switch node.NodeType {
	case tob.NODE_UNIT:
		return datastore.SaveStruct(node.unit)
	case tob.NODE_GROUP:
		return datastore.SaveStruct(node.group)
	default:
		panic("undefined node type")
	}
}

func (node *TbNode) LoadKey(k *datastore.Key) error {
	node.Key = k
	return nil
}

func (node *TbNode) ToInternal() tob.Node {
	return tob.Node{
		Type:  node.NodeType,
		Unit:  FromDatastoreUnit(node.unit),
		Group: FromDatastoreGroup(node.group),
	}
}
