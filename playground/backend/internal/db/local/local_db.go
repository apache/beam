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

package local_db

import (
	"beam.apache.org/playground/backend/internal/db/entity"
	"context"
	"fmt"
	"sync"
)

type LocalDB struct {
	mu    sync.RWMutex
	items map[string]interface{}
}

func New() (*LocalDB, error) {
	items := make(map[string]interface{})
	ls := &LocalDB{items: items}
	return ls, nil
}

// PutSnippet puts the entity to the local map
func (l *LocalDB) PutSnippet(_ context.Context, id string, snip *entity.Snippet) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items[id] = snip
	return nil
}

// GetSnippet returns the snippet entity
func (l *LocalDB) GetSnippet(_ context.Context, id string) (*entity.SnippetEntity, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	value, found := l.items[id]
	if !found {
		return nil, fmt.Errorf("value with id: %s not found", id)
	}
	snippet, _ := value.(*entity.Snippet)
	return snippet.Snippet, nil
}

// PutSchemaVersion puts the schema entity to the local map
func (l *LocalDB) PutSchemaVersion(_ context.Context, id string, schema *entity.SchemaEntity) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items[id] = schema
	return nil
}

// PutSDKs puts the SDK entities to the local map
func (l *LocalDB) PutSDKs(_ context.Context, sdks []*entity.SDKEntity) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, sdk := range sdks {
		l.items[sdk.Name] = sdk
	}
	return nil
}

//GetFiles returns the file entities by a snippet identifier
func (l *LocalDB) GetFiles(_ context.Context, snipId string, _ int) ([]*entity.FileEntity, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	value, found := l.items[snipId]
	if !found {
		return nil, fmt.Errorf("value with id: %s not found", snipId)
	}
	snippet, _ := value.(*entity.Snippet)
	return snippet.Files, nil
}

//GetSDK returns the sdk entity by an identifier
func (l *LocalDB) GetSDK(_ context.Context, id string) (*entity.SDKEntity, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	value, found := l.items[id]
	if !found {
		return nil, fmt.Errorf("value with id: %s not found", id)
	}
	sdk, _ := value.(*entity.SDKEntity)
	return sdk, nil
}
