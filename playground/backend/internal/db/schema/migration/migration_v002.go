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
)

type AddingComplexityProperty struct {
}

func (is *AddingComplexityProperty) InitiateData(args *schema.DBArgs) error {
	schemaEntity := &entity.SchemaEntity{Descr: is.GetDescription()}
	if err := args.Db.PutSchemaVersion(args.Ctx, is.GetVersion(), schemaEntity); err != nil {
		return err
	}
	return nil
}

func (is *AddingComplexityProperty) GetVersion() string {
	return "0.0.2"
}

func (is AddingComplexityProperty) GetDescription() string {
	return "Adding a complexity property to the example entity"
}
