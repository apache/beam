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

package test_cleaner

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"

	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/utils"
)

func CleanExample(ctx context.Context, t *testing.T, exampleId string) {
	cleanData(ctx, t, constants.ExampleKind, exampleId, nil)
}

func CleanSnippet(ctx context.Context, t *testing.T, snippetId string) {
	cleanData(ctx, t, constants.SnippetKind, snippetId, nil)
}

func CleanPCObjs(ctx context.Context, t *testing.T, exampleId string) {
	pcTypes := []string{constants.PCOutputType, constants.PCLogType, constants.PCGraphType}
	for _, pcType := range pcTypes {
		cleanData(ctx, t, constants.PCObjectKind, utils.GetIDWithDelimiter(exampleId, pcType), nil)
	}
}

func CleanFiles(ctx context.Context, t *testing.T, snippetId string, numberOfFiles int) {
	for fileIndx := 0; fileIndx < numberOfFiles; fileIndx++ {
		cleanData(ctx, t, constants.FileKind, utils.GetIDWithDelimiter(snippetId, fileIndx), nil)
	}
}

func CleanSchemaVersion(ctx context.Context, t *testing.T, schemaId string) {
	cleanData(ctx, t, constants.SchemaKind, schemaId, nil)
}

func cleanData(ctx context.Context, t *testing.T, kind, id string, parentId *datastore.Key) {
	client, err := datastore.NewClient(ctx, constants.EmulatorProjectId)
	if err != nil {
		t.Errorf("Error during datastore client creating, err: %s\n", err.Error())
		return
	}
	defer func() {
		err := client.Close()
		if err != nil {
			t.Errorf("Error during datastore client closing, err: %s\n", err.Error())
		}
	}()
	key := datastore.NameKey(kind, id, nil)
	if parentId != nil {
		key.Parent = parentId
	}
	key.Namespace = utils.GetNamespace(ctx)
	if err = client.Delete(ctx, key); err != nil {
		t.Errorf("Error during data cleaning, err: %s", err.Error())
	}
}
