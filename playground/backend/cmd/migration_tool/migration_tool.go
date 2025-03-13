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

package main

import (
	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/mapper"
	"beam.apache.org/playground/backend/internal/db/schema"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"flag"
	"fmt"
	"os"
)

func main() {
	projectId := flag.String("project-id", "", "GCP project id")
	sdkConfigPath := flag.String("sdk-config", "", "Path to the sdk config file")
	namespace := flag.String("namespace", constants.Namespace, "Datastore namespace")

	flag.Parse()

	ctx := context.WithValue(context.Background(), constants.DatastoreNamespaceKey, *namespace)

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Couldn't get the current working directory, err: %s \n", err.Error())
		os.Exit(1)
	}
	logger.SetupLogger(context.Background(), cwd, *projectId)

	migratedDb, err := datastore.New(ctx, mapper.NewPrecompiledObjectMapper(), nil, *projectId)
	if err != nil {
		logger.Fatalf("Couldn't create DB client instance, err: %s \n", err.Error())
		os.Exit(1)
	}

	if err := migratedDb.ApplyMigrations(ctx, schema.Migrations, *sdkConfigPath); err != nil {
		logger.Fatalf("Couldn't apply migrations, err: %s \n", err.Error())
		os.Exit(1)
	}
}
