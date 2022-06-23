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

package migration

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/schema"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	versionV1     = "0.0.1"
	descriptionV1 = "Data initialization: file snippet, schema versions, SDK"
)

type InitialStructure struct {
}

func (is *InitialStructure) InitiateData(args *schema.DBArgs) error {
	//init snippets
	dummyStr := "dummy"
	idInfo := entity.IDInfo{
		IdLength: args.AppEnv.IdLength(),
		Salt:     args.AppEnv.PlaygroundSalt(),
	}
	snip := &entity.Snippet{
		IDInfo: &idInfo,
		Snippet: &entity.SnippetEntity{
			OwnerId:  dummyStr,
			PipeOpts: dummyStr,
		},
		Files: []*entity.FileEntity{
			{
				Name:    dummyStr,
				Content: dummyStr,
			},
		},
	}
	snipId, err := snip.ID()
	if err != nil {
		return err
	}
	if err = args.Db.PutSnippet(args.Ctx, snipId, snip); err != nil {
		return err
	}

	//init schema versions
	schemaEntity := &entity.SchemaEntity{Descr: descriptionV1}
	if err = args.Db.PutSchemaVersion(args.Ctx, versionV1, schemaEntity); err != nil {
		return err
	}

	//init sdks
	var sdkEntities []*entity.SDKEntity
	sdkConfig := new(SdkConfig)
	if err = utils.ReadYamlFile(args.AppEnv.SdkConfigPath(), sdkConfig); err != nil {
		return err
	}
	for _, sdk := range pb.Sdk_name {
		if sdk == pb.Sdk_SDK_UNSPECIFIED.String() {
			continue
		}
		defaultExample := getDefaultExample(sdkConfig, sdk)
		sdkEntities = append(sdkEntities, &entity.SDKEntity{
			Name:           sdk,
			DefaultExample: defaultExample,
		})
	}
	if err = args.Db.PutSDKs(args.Ctx, sdkEntities); err != nil {
		return err
	}

	return nil
}

type SdkConfig struct {
	Sdks struct {
		Go     *SdkProperties `yaml:"SDK_GO"`
		Java   *SdkProperties `yaml:"SDK_JAVA"`
		Python *SdkProperties `yaml:"SDK_PYTHON"`
		Scio   *SdkProperties `yaml:"SDK_SCIO"`
	}
}

type SdkProperties struct {
	DefaultExample string `yaml:"default-example"`
}

func getDefaultExample(config *SdkConfig, sdk string) string {
	switch sdk {
	case pb.Sdk_SDK_JAVA.String():
		return config.Sdks.Java.DefaultExample
	case pb.Sdk_SDK_GO.String():
		return config.Sdks.Go.DefaultExample
	case pb.Sdk_SDK_PYTHON.String():
		return config.Sdks.Python.DefaultExample
	case pb.Sdk_SDK_SCIO.String():
		return config.Sdks.Scio.DefaultExample
	default:
		return ""
	}
}

func (is *InitialStructure) GetVersion() string {
	return versionV1
}
