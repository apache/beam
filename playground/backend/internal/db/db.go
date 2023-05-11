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
	"beam.apache.org/playground/backend/internal/db/datastore"
	"context"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/db/entity"
)

type Database interface {
	SnippetDatabase
	CatalogDatabase
	ExampleDatabase
	MigrationsDatabase
}

type SnippetDatabase interface {
	PutSnippet(ctx context.Context, id string, snip *entity.Snippet) error

	GetSnippet(ctx context.Context, id string) (*entity.SnippetEntity, error)

	GetFiles(ctx context.Context, snipId string, numberOfFiles int) ([]*entity.FileEntity, error)

	DeleteUnusedSnippets(ctx context.Context, retentionPeriod time.Duration) error
}

type CatalogDatabase interface {
	GetSDKs(ctx context.Context) ([]*entity.SDKEntity, error)
}

type ExampleDatabase interface {
	GetCatalog(ctx context.Context, sdkCatalog []*entity.SDKEntity) ([]*pb.Categories, error)

	GetDefaultExamples(ctx context.Context, sdks []*entity.SDKEntity) (map[pb.Sdk]*pb.PrecompiledObject, error)

	GetExample(ctx context.Context, id string, sdks []*entity.SDKEntity) (*pb.PrecompiledObject, error)

	GetExampleCode(ctx context.Context, id string) ([]*entity.FileEntity, error)

	GetExampleOutput(ctx context.Context, id string) (string, error)

	GetExampleLogs(ctx context.Context, id string) (string, error)

	GetExampleGraph(ctx context.Context, id string) (string, error)
}

type MigrationsDatabase interface {
	GetCurrentDbMigrationVersion(ctx context.Context) (int, error)

	ApplyMigrations(ctx context.Context, migrations []datastore.Migration, sdkConfigPath string) error
}
