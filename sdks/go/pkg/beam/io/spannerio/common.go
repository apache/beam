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

// Package spannerio provides an API for reading and writing resouces to
// Google Spanner datastores.
package spannerio

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
)

type SpannerDatabase struct {
	Database string `json:"database"` // Database is the spanner connection string

	Client *spanner.Client // Spanner Client
}

func WithDatabase(database string) *SpannerDatabase {
	return &SpannerDatabase{
		Database: database,
	}
}

func (db *SpannerDatabase) Setup(ctx context.Context) error {
	if db.Client == nil {
		client, err := spanner.NewClient(ctx, db.Database)
		if err != nil {
			return fmt.Errorf("failed to initialise Spanner client: %v", err)
		}

		db.Client = client
	}

	return nil
}

func (db *SpannerDatabase) Close() {
	if db.Client != nil {
		db.Client.Close()
	}
}
