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

package storage

import (
	"context"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

type Iface interface {
	GetContentTree(ctx context.Context, sdk tob.Sdk) (tob.ContentTree, error)
	SaveContentTrees(ctx context.Context, trees []tob.ContentTree) error

	GetUnitContent(ctx context.Context, sdk tob.Sdk, unitId string) (*tob.Unit, error)
	// Check if the unit exists, returns ErrNoUnit if not
	CheckUnitExists(ctx context.Context, sdk tob.Sdk, unitId string) error

	SaveUser(ctx context.Context, uid string) error
	GetUserProgress(ctx context.Context, sdk tob.Sdk, uid string) (*tob.SdkProgress, error)
	SetUnitComplete(ctx context.Context, sdk tob.Sdk, unitId, uid string) error

	SaveUserSnippetId(
		ctx context.Context, sdk tob.Sdk, unitId, uid string,
		externalSave func(string) (string, error),
	) error

	DeleteProgress(ctx context.Context, uid string) error
}
