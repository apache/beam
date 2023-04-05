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

package backend

import (
	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
	"beam.apache.org/playground/backend/playground_functions"
	"context"
	"fmt"
	"net/http"
	"time"

	"beam.apache.org/playground/backend/internal/db/mapper"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

const retentionPeriod = 100 * time.Hour * 24

var db *datastore.Datastore

func init() {
	env := playground_functions.GetEnvironment()
	logger.SetupLogger(context.Background(), "", env.GetProjectId())

	logger.Debugf("Initializing snippets functions\n")

	pcMapper := mapper.NewPrecompiledObjectMapper()
	var err error
	db, err = datastore.New(context.Background(), pcMapper, nil, env.GetProjectId())
	if err != nil {
		fmt.Printf("Couldn't create the database client, err: %s\n", err.Error())
		panic(err)
	}

	ensurePost := playground_functions.EnsureMethod(http.MethodPost)

	functions.HTTP("cleanupSnippets", ensurePost(cleanupSnippets))
	functions.HTTP("deleteObsoleteSnippets", ensurePost(deleteObsoleteSnippets))
	functions.HTTP("incrementSnippetViews", ensurePost(incrementSnippetViews))
}

func handleError(w http.ResponseWriter, statusCode int, err error) {
	// Return 500 error and error message
	w.WriteHeader(statusCode)
	_, werr := w.Write([]byte(err.Error()))
	if werr != nil {
		logger.Errorf("Couldn't write error message, err: %s \n", werr.Error())
	}
}

// cleanupSnippets removes old snippets from the database.
func cleanupSnippets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	err := db.DeleteUnusedSnippets(ctx, retentionPeriod)
	if err != nil {
		logger.Errorf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func deleteObsoleteSnippets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	snipId := r.URL.Query().Get("snipId")
	persistenceKey := r.URL.Query().Get("persistenceKey")

	snipKey := utils.GetSnippetKey(ctx, snipId)
	err := db.DeleteObsoleteSnippets(ctx, snipKey, persistenceKey)
	if err != nil {
		logger.Errorf("Couldn't delete obsolete code snippets for snipId %s, persistenceKey %s, err: %s \n", snipId, persistenceKey, err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func incrementSnippetViews(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	snipId := r.URL.Query().Get("snipId")

	err := db.IncrementSnippetVisitorsCount(ctx, snipId)
	if err != nil {
		logger.Errorf("Couldn't increment snippet visitors count for snipId %s, err: %s \n", snipId, err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}
}
