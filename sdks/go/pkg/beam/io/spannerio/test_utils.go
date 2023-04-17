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

package spannerio

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	db "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/spannertest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

type TestDto struct {
	One string `spanner:"One"`
	Two int64  `spanner:"Two"`
}

func newServer(t *testing.T) (*spannertest.Server, func()) {
	srv, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("Starting in-memory fake spanner: %v", err)
	}

	return srv, func() {
		srv.Close()
	}
}

func createFakeClient(address string, database string) (*spanner.Client, *db.DatabaseAdminClient, func(), error) {
	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure())
	if err != nil {
		return nil, nil, nil, err
	}

	client, err := spanner.NewClient(ctx, database, option.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, nil, err
	}

	admin, err := db.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, nil, err
	}

	return client, admin, func() {
		client.Close()
		admin.Close()
		conn.Close()
	}, nil
}
