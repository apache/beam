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

package db

import (
	"beam.apache.org/playground/backend/internal/db/entity"
	"context"
)

type Database interface {
	Snippet
	Catalogue
}

type Snippet interface {
	PutSnippet(ctx context.Context, id string, snip *entity.Snippet) error

	GetSnippet(ctx context.Context, id string) (*entity.SnippetEntity, error)

	GetFiles(ctx context.Context, parentId string) ([]*entity.FileEntity, error)
}

type Catalogue interface {
	PutSchemaVersion(ctx context.Context, id string, schema *entity.SchemaEntity) error

	PutSDKs(ctx context.Context, sdks []*entity.SDKEntity) error

	GetSDK(ctx context.Context, id string) (*entity.SDKEntity, error)
}
