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
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/mapper"
)

func createDatastoreClient(ctx context.Context, projectId string) (*datastore.Datastore, error) {
	pcMapper := mapper.NewPrecompiledObjectMapper()
	db, err := datastore.New(ctx, pcMapper, nil, projectId)
	if err != nil {
		logger.Errorf("Couldn't create the database client, err: %s \n", err.Error())
		return nil, err
	}

	return db, nil
}

func cleanup(dayDiff int, projectId, namespace string) error {
	logger.Infof("Removing unused snippets is running...")
	startDate := time.Now()

	ctx := context.WithValue(context.Background(), constants.DatastoreNamespaceKey, namespace)
	db, err := createDatastoreClient(ctx, projectId)
	if err != nil {
		return err
	}

	retentionPeriod := time.Duration(dayDiff) * 24 * time.Hour
	err = db.DeleteUnusedSnippets(ctx, retentionPeriod)
	if err != nil {
		logger.Errorf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		return err
	}

	diffTime := time.Now().Sub(startDate).Milliseconds()
	logger.Infof("Removing unused snippets finished, work time: %d ms\n", diffTime)

	return nil
}

func remove(snippetId string, projectId, namespace string) error {
	logger.Infof("Removing snippet %s is running...", snippetId)
	startDate := time.Now()

	ctx := context.WithValue(context.Background(), constants.DatastoreNamespaceKey, namespace)
	db, err := createDatastoreClient(ctx, projectId)
	if err != nil {
		return err
	}

	err = db.DeleteSnippet(ctx, snippetId)
	if err != nil {
		fmt.Printf("Couldn't delete code snippet, err: %s \n", err.Error())
		return err
	}

	diffTime := time.Now().Sub(startDate).Milliseconds()
	logger.Infof("Removing snippet %s finished, work time: %d ms\n", snippetId, diffTime)

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("expected 'cleanup' or 'remove' subcommands")
		os.Exit(1)
	}

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Printf("Couldn't get the current working directory, err: %s \n", err.Error())
		os.Exit(1)
	}

	switch os.Args[1] {
	case "cleanup":
		cleanupCmd := flag.NewFlagSet("cleanup", flag.ExitOnError)
		dayDiff := cleanupCmd.Int("day_diff", -1, "a number of days to keep the code snippets")
		projectId := cleanupCmd.String("project_id", "", "a project id where the code snippets are stored")
		namespace := cleanupCmd.String("namespace", constants.Namespace, "a Datastore namespace where the code snippets are stored")

		err := cleanupCmd.Parse(os.Args[2:])
		if err != nil {
			fmt.Printf("Couldn't parse the command line arguments, err: %s \n", err.Error())
			os.Exit(1)
		}

		if projectId == nil || *projectId == "" {
			fmt.Println("project_id is required")
			os.Exit(1)
		}

		if *dayDiff < 0 {
			fmt.Println("day_diff is required and should be greater than 0")
			os.Exit(1)
		}

		logger.SetupLogger(context.Background(), cwd, *projectId)
		err = cleanup(*dayDiff, *projectId, *namespace)
		if err != nil {
			logger.Fatalf("Couldn't cleanup the database, err: %s \n", err.Error())
			os.Exit(1)
		}
	case "remove":
		removeCmd := flag.NewFlagSet("remove", flag.ExitOnError)
		snippetId := removeCmd.String("snippet_id", "", "a snippet id to remove")
		projectId := removeCmd.String("project_id", "", "a project id where the code snippets are stored")
		namespace := removeCmd.String("namespace", constants.Namespace, "a Datastore namespace where the code snippets are stored")

		err := removeCmd.Parse(os.Args[2:])
		if err != nil {
			fmt.Printf("Couldn't parse the command line arguments, err: %s \n", err.Error())
			os.Exit(1)
		}

		if projectId == nil || *projectId == "" {
			fmt.Println("project_id is required")
			os.Exit(1)
		}

		if snippetId == nil || *snippetId == "" {
			fmt.Println("snippet_id is required")
			os.Exit(1)
		}

		logger.SetupLogger(context.Background(), cwd, *projectId)
		err = remove(*snippetId, *projectId, *namespace)
		if err != nil {
			logger.Fatalf("Couldn't remove the code snippet, err: %s \n", err.Error())
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown subcommand %s. Expected 'cleanup' or 'remove'\n", os.Args[1])
		os.Exit(1)
	}
}
