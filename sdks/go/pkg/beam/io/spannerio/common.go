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
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type spannerFn struct {
	Database     string          `json:"database"` // Database is the spanner connection string
	TestEndpoint string          // Optional endpoint override for local testing. Not required for production pipelines.
	client       *spanner.Client // Spanner Client
}

func newSpannerFn(db string) spannerFn {
	if db == "" {
		panic("database not provided!")
	}

	return spannerFn{
		Database: db,
	}
}

func (f *spannerFn) Setup(ctx context.Context) error {
	if f.client == nil {
		var opts []option.ClientOption

		// Append emulator options assuming endpoint is local (for testing).
		if f.TestEndpoint != "" {
			opts = []option.ClientOption{
				option.WithEndpoint(f.TestEndpoint),
				option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
				option.WithoutAuthentication(),
				internaloption.SkipDialSettingsValidation(),
			}
		}

		client, err := spanner.NewClient(ctx, f.Database, opts...)
		if err != nil {
			return fmt.Errorf("failed to initialise Spanner client: %v", err)
		}

		f.client = client
	}

	return nil
}

func (f *spannerFn) Teardown() {
	if f.client != nil {
		f.client.Close()
	}
}
