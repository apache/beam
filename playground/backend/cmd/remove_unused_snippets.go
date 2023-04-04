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
	db, err := datastore.New(ctx, pcMapper, projectId)
	if err != nil {
		fmt.Printf("Couldn't create the database client, err: %s \n", err.Error())
		return nil, err
	}

	return db, nil
}

func cleanup(dayDiff int, projectId string) error {
	ctx := context.Background()
	db, err := createDatastoreClient(ctx, projectId)
	if err != nil {
		return err
	}

	err = db.DeleteUnusedSnippets(ctx, int32(dayDiff))
	if err != nil {
		fmt.Printf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		return err
	}

	return nil
}

func remove(snippetId string, projectId string) error {
	ctx := context.Background()
	db, err := createDatastoreClient(ctx, projectId)
	if err != nil {
		return err
	}

	err = db.DeleteSnippet(ctx, snippetId)
	if err != nil {
		fmt.Printf("Couldn't delete code snippet, err: %s \n", err.Error())
		return err
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("expected 'cleanup' or 'remove' subcommands")
		os.Exit(1)
	}

	fmt.Println("Removing unused snippets is running...")
	startDate := time.Now()

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
		err = cleanup(*dayDiff, *projectId)
		if err != nil {
			fmt.Printf("Couldn't cleanup the database, err: %s \n", err.Error())
			os.Exit(1)
		}
	case "remove":
		removeCmd := flag.NewFlagSet("remove", flag.ExitOnError)
		snippetId := removeCmd.String("snippet_id", "", "a snippet id to remove")
		projectId := removeCmd.String("project_id", "", "a project id where the code snippets are stored")

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
		err = remove(*snippetId, *projectId)
		if err != nil {
			fmt.Printf("Couldn't remove the code snippet, err: %s \n", err.Error())
			os.Exit(1)
		}
	}

	diffTime := time.Now().Sub(startDate).Milliseconds()
	fmt.Printf("Removing unused snippets finished, work time: %d ms\n", diffTime)
}
