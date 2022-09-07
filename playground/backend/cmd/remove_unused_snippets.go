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
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/mapper"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Go must have at least three arguments")
		return
	}
	dayDiff := os.Args[1]
	projectId := os.Args[2]
	fmt.Println("Removing unused snippets is running...")
	startDate := time.Now()

	diff, err := strconv.Atoi(dayDiff)
	if err != nil {
		fmt.Printf("Couldn't convert days to integer from the input parameter, err: %s \n", err.Error())
		return
	}

	ctx := context.Background()
	pcMapper := mapper.NewPrecompiledObjectMapper()
	db, err := datastore.New(ctx, pcMapper, projectId)
	if err != nil {
		fmt.Printf("Couldn't create the database client, err: %s \n", err.Error())
		return
	}

	err = db.DeleteUnusedSnippets(ctx, int32(diff))
	if err != nil {
		fmt.Printf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		return
	}

	diffTime := time.Now().Sub(startDate).Milliseconds()
	fmt.Printf("Removing unused snippets finished, work time: %d ms\n", diffTime)
}
