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
package test_scripts

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/datastore"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/constants"
	ds "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/utils"
)

func DownloadCatalogsWithMockData(ctx context.Context) {
	projectId := getEnv("GOOGLE_CLOUD_PROJECT", "test")
	dbClient, err := ds.New(ctx, mapper.NewPrecompiledObjectMapper(), projectId)
	if err != nil {
		fmt.Println(err.Error())
	}

	sdks := createSDKEntities()
	err = dbClient.PutSDKs(ctx, sdks)
	if err != nil {
		fmt.Println(err.Error())
	}

	exampleKeys, examples := createExampleEntities()
	_, err = dbClient.Client.PutMulti(ctx, exampleKeys, examples)
	if err != nil {
		fmt.Println(err.Error())
	}

	snippetKeys, snippets := createSnippetEntities(examples)
	_, err = dbClient.Client.PutMulti(ctx, snippetKeys, snippets)
	if err != nil {
		fmt.Println(err.Error())
	}

	fileKeys, files := createFileEntities(examples)
	_, err = dbClient.Client.PutMulti(ctx, fileKeys, files)
	if err != nil {
		fmt.Println(err.Error())
	}

	objKeys, objs := createPCObjEntities(examples)
	_, err = dbClient.Client.PutMulti(ctx, objKeys, objs)
	if err != nil {
		fmt.Println(err.Error())
	}

	err = dbClient.Client.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func createSDKEntities() []*entity.SDKEntity {
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
	return sdks
}

func createExampleEntities() ([]*datastore.Key, []*entity.ExampleEntity) {
	names := []string{"MOCK_DEFAULT_EXAMPLE", "MOCK_NAME_1", "MOCK_NAME_2", "MOCK_NAME_3"}
	keys := make([]*datastore.Key, 0)
	examples := make([]*entity.ExampleEntity, 0)
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		for _, name := range names {
			key := utils.GetExampleKey(sdk + "_" + name)
			keys = append(keys, key)
			example := createExampleEntity(name, sdk)
			examples = append(examples, example)
		}
	}
	return keys, examples
}

func createExampleEntity(name, sdk string) *entity.ExampleEntity {
	return &entity.ExampleEntity{
		Name:       name,
		Sdk:        utils.GetSdkKey(sdk),
		Descr:      "MOCK_DESCR",
		Tags:       []string{"MOCK_TAG_1", "MOCK_TAG_2", "MOCK_TAG_3"},
		Cats:       []string{"MOCK_CAT_1", "MOCK_CAT_2", "MOCK_CAT_3"},
		Complexity: 10,
		Path:       "MOCK_PATH",
		Type:       "PRECOMPILED_OBJECT_TYPE_EXAMPLE",
		Origin:     "PG_EXAMPLES",
		SchVer:     utils.GetSchemaVerKey("MOCK_VERSION"),
	}
}

func createSnippetEntities(examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.SnippetEntity) {
	keys := make([]*datastore.Key, 0)
	snippets := make([]*entity.SnippetEntity, 0)
	now := time.Now()
	for _, example := range examples {
		key := utils.GetSnippetKey(example.Sdk.Name + "_" + example.Name)
		snippet := &entity.SnippetEntity{
			Sdk:           example.Sdk,
			PipeOpts:      "MOCK_P_OPTS",
			Created:       now,
			Origin:        constants.ExampleOrigin,
			SchVer:        utils.GetSchemaVerKey("MOCK_VERSION"),
			NumberOfFiles: 1,
		}
		keys = append(keys, key)
		snippets = append(snippets, snippet)
	}

	return keys, snippets
}

func createFileEntities(examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.FileEntity) {
	keys := make([]*datastore.Key, 0)
	files := make([]*entity.FileEntity, 0)
	for _, example := range examples {
		key := utils.GetFileKey(example.Sdk.Name + "_" + example.Name + "_" + "0")
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

func createPCObjEntities(examples []*entity.ExampleEntity) ([]*datastore.Key, []*entity.PrecompiledObjectEntity) {
	keys := make([]*datastore.Key, 0)
	objs := make([]*entity.PrecompiledObjectEntity, 0)
	for _, example := range examples {
		types := []string{constants.PCLogType, constants.PCGraphType, constants.PCOutputType}
		for _, typeVal := range types {
			key := utils.GetPCObjectKey(example.Sdk.Name + "_" + example.Name + "_" + typeVal)
			obj := &entity.PrecompiledObjectEntity{Content: "MOCK_CONTENT_" + typeVal}
			keys = append(keys, key)
			objs = append(objs, obj)
		}
	}
	return keys, objs
}

func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}
