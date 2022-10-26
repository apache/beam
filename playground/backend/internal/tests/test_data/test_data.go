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

/*
	This is a golang script to download the example catalog and the sdk catalog
	to the Cloud Datastore for local deployment and testing.
	Please be aware that this is fictitious data.
*/
package test_data

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/datastore"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
)

func DownloadCatalogsWithMockData(ctx context.Context) {
	client, closeClientFunc := createDatastoreClient(ctx)
	defer closeClientFunc()

	sdkKeys, sdks := createSDKEntities(ctx)
	saveEntities(ctx, client, sdkKeys, sdks)

	exampleKeys, examples := createExampleEntities(ctx)
	saveEntities(ctx, client, exampleKeys, examples)

	snippetKeys, snippets := createSnippetEntities(ctx, examples)
	saveEntities(ctx, client, snippetKeys, snippets)

	fileKeys, files := createFileEntities(ctx, examples)
	saveEntities(ctx, client, fileKeys, files)

	objKeys, objs := createPCObjEntities(ctx, examples)
	saveEntities(ctx, client, objKeys, objs)
}

func RemoveCatalogsWithMockData(ctx context.Context) {
	client, closeClientFunc := createDatastoreClient(ctx)
	defer closeClientFunc()

	exampleKeys, examples := createExampleEntities(ctx)
	snippetKeys, _ := createSnippetEntities(ctx, examples)
	fileKeys, _ := createFileEntities(ctx, examples)
	objKeys, _ := createPCObjEntities(ctx, examples)

	deleteEntities(ctx, client, exampleKeys)
	deleteEntities(ctx, client, snippetKeys)
	deleteEntities(ctx, client, fileKeys)
	deleteEntities(ctx, client, objKeys)
}

func createSDKEntities(ctx context.Context) ([]*datastore.Key, []*entity.SDKEntity) {
	sdks := make([]*entity.SDKEntity, 0)
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		sdks = append(sdks, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: "MOCK_DEFAULT_EXAMPLE",
		})
	}
	keys := make([]*datastore.Key, 0, len(sdks))
	for _, sdk := range sdks {
		keys = append(keys, utils.GetSdkKey(ctx, sdk.Name))
	}
	return keys, sdks
}

func createExampleEntities(ctx context.Context) ([]*datastore.Key, []*entity.ExampleEntity) {
	names := []string{"MOCK_DEFAULT_EXAMPLE", "MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"}
	keys := make([]*datastore.Key, 0)
	examples := make([]*entity.ExampleEntity, 0)
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		for _, name := range names {
			key := utils.GetExampleKey(ctx, sdk, name)
			keys = append(keys, key)
			example := createExampleEntity(ctx, name, sdk)
			examples = append(examples, example)
		}
	}
	return keys, examples
}

func createExampleEntity(ctx context.Context, name, sdk string) *entity.ExampleEntity {
	return &entity.ExampleEntity{
		Name:   name,
		Sdk:    utils.GetSdkKey(ctx, sdk),
		Descr:  "MOCK_DESCR",
		Tags:   []string{"MOCK_TAG_1", "MOCK_TAG_2", "MOCK_TAG_3"},
		Cats:   []string{"MOCK_CAT_1", "MOCK_CAT_2", "MOCK_CAT_3"},
		Path:   "MOCK_PATH",
		Type:   pb.PrecompiledObjectType_PRECOMPILED_OBJECT_TYPE_EXAMPLE.String(),
		Origin: constants.ExampleOrigin,
		SchVer: utils.GetSchemaVerKey(ctx, "MOCK_VERSION"),
	}
}

func createSnippetEntities(ctx context.Context, examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.SnippetEntity) {
	keys := make([]*datastore.Key, 0)
	snippets := make([]*entity.SnippetEntity, 0)
	now := time.Now()
	for _, example := range examples {
		key := utils.GetSnippetKey(ctx, example.Sdk.Name, example.Name)
		snippet := &entity.SnippetEntity{
			Sdk:           example.Sdk,
			PipeOpts:      "MOCK_P_OPTS",
			Created:       now,
			Origin:        constants.ExampleOrigin,
			SchVer:        utils.GetSchemaVerKey(ctx, "MOCK_VERSION"),
			NumberOfFiles: 1,
			Complexity:    pb.Complexity_COMPLEXITY_MEDIUM.String(),
		}
		keys = append(keys, key)
		snippets = append(snippets, snippet)
	}

	return keys, snippets
}

func createFileEntities(ctx context.Context, examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.FileEntity) {
	keys := make([]*datastore.Key, 0)
	files := make([]*entity.FileEntity, 0)
	for _, example := range examples {
		key := utils.GetFileKey(ctx, example.Sdk.Name, example.Name, 0)
		file := &entity.FileEntity{
			Name:     "MOCK_NAME",
			Content:  "MOCK_CONTENT",
			CntxLine: 10,
			IsMain:   true,
		}
		keys = append(keys, key)
		files = append(files, file)
	}
	return keys, files
}

func createPCObjEntities(ctx context.Context, examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.PrecompiledObjectEntity) {
	keys := make([]*datastore.Key, 0)
	objs := make([]*entity.PrecompiledObjectEntity, 0)
	for _, example := range examples {
		types := []string{constants.PCLogType, constants.PCGraphType, constants.PCOutputType}
		for _, typeVal := range types {
			key := utils.GetPCObjectKey(ctx, example.Sdk.Name, example.Name, typeVal)
			obj := &entity.PrecompiledObjectEntity{Content: "MOCK_CONTENT_" + typeVal}
			keys = append(keys, key)
			objs = append(objs, obj)
		}
	}
	return keys, objs
}

func deleteEntities(ctx context.Context, client *datastore.Client, keys []*datastore.Key) {
	err := client.DeleteMulti(ctx, keys)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func saveEntities(ctx context.Context, client *datastore.Client, keys []*datastore.Key, entities interface{}) {
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func createDatastoreClient(ctx context.Context) (*datastore.Client, func()) {
	projectId := getEnv("GOOGLE_CLOUD_PROJECT", constants.EmulatorProjectId)
	client, err := datastore.NewClient(ctx, projectId)
	if err != nil {
		fmt.Println(err.Error())
	}
	closeClientFunc := func() {
		err := client.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}
	return client, closeClientFunc
}

func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}
