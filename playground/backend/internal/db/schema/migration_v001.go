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

package schema

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	ds "beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/utils"
	"cloud.google.com/go/datastore"
	"context"
)

type migrationV001 struct {
	ds.MigrationBase
}

func GetMigration_v001() ds.Migration {
	return &migrationV001{
		MigrationBase: ds.MigrationBase{
			Version:     1,
			Description: "Data initialization: a schema version, SDKs",
		},
	}
}

func (m migrationV001) Apply(ctx context.Context, tx *datastore.Transaction, sdkConfigPath string) error {
	// Init sdks
	var sdkEntities []*entity.SDKEntity
	sdkConfig := new(SdkConfig)
	if err := utils.ReadYamlFile(sdkConfigPath, sdkConfig); err != nil {
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
	if err := ds.TxPutSDKs(ctx, tx, sdkEntities); err != nil {
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
