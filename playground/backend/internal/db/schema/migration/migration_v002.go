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
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/db/schema"
	"beam.apache.org/playground/backend/internal/logger"
)

type AddingComplexityProperty struct {
}

func (is AddingComplexityProperty) InitializeData(args *schema.DBArgs) error {
	// Verify if migration is already applied
	migrationApplied, err := args.Db.HasSchemaVersion(args.Ctx, is.GetVersion().String())
	if err != nil {
		logger.Errorf("InitializeData(): error during checking migration version %s: %s", is.GetVersion().String(), err.Error())
		return err
	}

	if migrationApplied {
		return nil
	}

	logger.Infof("InitializeData(): applying migration version %s", is.GetVersion().String())

	// Apply migration
	schemaEntity := &entity.SchemaEntity{Descr: is.GetDescription()}
	if err := args.Db.PutSchemaVersion(args.Ctx, is.GetVersion().String(), schemaEntity); err != nil {
		return err
	}
	return nil
}

func (is AddingComplexityProperty) GetVersion() schema.DBVersion {
	return schema.DBVersion{
		Major: 0,
		Minor: 0,
		Patch: 2,
	}
}

func (is AddingComplexityProperty) GetDescription() string {
	return "Adding a complexity property to the example entity"
}
