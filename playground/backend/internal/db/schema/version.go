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
	"fmt"
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

func (ds *DBSchema) InitializeData() (*DBVersion, error) {
	var versions []DBVersion
	for _, ver := range ds.versions {
		if err := ver.InitializeData(ds.args); err != nil {
			logger.Errorf("DBSchema: InitiateData() error during the data initialization, err: %s", err.Error())
			return nil, err
		}
		versions = append(versions, ver.GetVersion())
	}
	sort.Sort(ByVersion(versions))
	return &versions[len(versions)-1], nil
}

type Version interface {
	GetVersion() DBVersion
	GetDescription() string
	InitializeData(args *DBArgs) error
}

type DBVersion struct {
	Major int
	Minor int
	Patch int
}

func (dv DBVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", dv.Major, dv.Minor, dv.Patch)
}

// ByVersion implements sort.Interface for []DBVersion based on the Major, Minor and Patch fields.
type ByVersion []DBVersion

func (bv ByVersion) Len() int      { return len(bv) }
func (bv ByVersion) Swap(i, j int) { bv[i], bv[j] = bv[j], bv[i] }
func (bv ByVersion) Less(i, j int) bool {
	if bv[i].Major < bv[j].Major {
		return true
	}
	if bv[i].Major > bv[j].Major {
		return false
	}
	if bv[i].Minor < bv[j].Minor {
		return true
	}
	if bv[i].Minor > bv[j].Minor {
		return false
	}
	if bv[i].Patch < bv[j].Patch {
		return true
	}
	return false
}
