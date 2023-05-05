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
	"beam.apache.org/playground/backend/internal/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"beam.apache.org/playground/backend/internal/db/datastore"
	"beam.apache.org/playground/backend/internal/db/entity"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/playground_functions"

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
	functions.HTTP("putSnippet", ensurePost(putSnippet))
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
	namespace := r.URL.Query().Get("namespace")
	ctx := context.WithValue(r.Context(), constants.DatastoreNamespaceKey, namespace)

	err := db.DeleteUnusedSnippets(ctx, retentionPeriod)
	if err != nil {
		logger.Errorf("Couldn't delete unused code snippets, err: %s \n", err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func putSnippet(w http.ResponseWriter, r *http.Request) {
	snipId := r.URL.Query().Get("snipId")
	namespace := r.URL.Query().Get("namespace")
	ctx := context.WithValue(r.Context(), constants.DatastoreNamespaceKey, namespace)

	var snip entity.Snippet
	err := json.NewDecoder(r.Body).Decode(&snip)
	if err != nil {
		logger.Errorf("Couldn't decode request body, err: %s \n", err.Error())
		handleError(w, http.StatusBadRequest, err)
		return
	}

	err = db.PutSnippetDirect(ctx, snipId, &snip)
	if err != nil {
		logger.Errorf("Couldn't put snippet to the database, err: %s \n", err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func incrementSnippetViews(w http.ResponseWriter, r *http.Request) {
	snipId := r.URL.Query().Get("snipId")
	namespace := r.URL.Query().Get("namespace")
	ctx := context.WithValue(r.Context(), constants.DatastoreNamespaceKey, namespace)

	err := db.IncrementSnippetVisitorsCount(ctx, snipId)
	if err != nil {
		logger.Errorf("Couldn't increment snippet visitors count for snipId %s, err: %s \n", snipId, err.Error())
		handleError(w, http.StatusInternalServerError, err)
		return
	}
}
