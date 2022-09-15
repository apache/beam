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
	"beam.apache.org/playground/backend/internal/db"
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"sort"
)

type DBArgs struct {
	Ctx    context.Context
	Db     db.Database
	AppEnv *environment.ApplicationEnvs
	Props  *environment.Properties
}

type DBSchema struct {
	args     *DBArgs
	versions []Version
}

func New(ctx context.Context, db db.Database, appEnv *environment.ApplicationEnvs, props *environment.Properties, versions []Version) *DBSchema {
	return &DBSchema{
		args:     &DBArgs{ctx, db, appEnv, props},
		versions: versions,
	}
}

func (ds *DBSchema) InitiateData() (string, error) {
	var versions []string
	for _, ver := range ds.versions {
		if err := ver.InitiateData(ds.args); err != nil {
			logger.Errorf("DBSchema: InitiateData() error during the data initialization, err: %s", err.Error())
			return "", err
		}
		versions = append(versions, ver.GetVersion())
	}
	sort.Strings(versions)
	return versions[len(versions)-1], nil
}

type Version interface {
	GetVersion() string
	GetDescription() string
	InitiateData(args *DBArgs) error
}
