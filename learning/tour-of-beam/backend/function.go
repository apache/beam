// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:generate moq -rm -out playground_api/api/v1/mock.go playground_api/api/v1 PlaygroundServiceClient

package tob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/service"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	pb "beam.apache.org/learning/tour-of-beam/backend/playground_api/api/v1"
	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_status "google.golang.org/grpc/status"
)

var (
	svc      service.IContent
	auth     *Authorizer
	pgClient pb.PlaygroundServiceClient
)

// Helper to format http error messages.
func finalizeErrResponse(w http.ResponseWriter, status int, code, message string) {
	resp := tob.CodeMessage{Code: code, Message: message}

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func MakeRepo(ctx context.Context) storage.Iface {
	// dependencies
	// required:
	// * TOB_MOCK: respond with static samples
	// OR
	// * GOOGLE_APPLICATION_CREDENTIALS: json file path to cloud credentials
	// * DATASTORE_PROJECT_ID: cloud project id
	// optional:
	// * DATASTORE_EMULATOR_HOST: emulator host/port (ex. 0.0.0.0:8888)
	if os.Getenv("TOB_MOCK") > "" {
		fmt.Println("Initialize mock storage")
		return &storage.Mock{}
	} else {
		// consumes DATASTORE_* env variables
		client, err := datastore.NewClient(ctx, "")
		if err != nil {
			log.Fatalf("new datastore client: %v", err)
		}

		return &storage.DatastoreDb{Client: client}
	}
}

func MakePlaygroundClient(ctx context.Context) pb.PlaygroundServiceClient {
	// dependencies
	// required:
	// * TOB_MOCK: use mock implementation
	// OR
	// * PLAYGROUND_ROUTER_HOST: playground API host/port
	if os.Getenv("TOB_MOCK") > "" {
		fmt.Println("Using mock playground client")
		return service.GetMockClient()
	} else {
		host := os.Getenv("PLAYGROUND_ROUTER_HOST")
		cc, err := grpc.Dial(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("fail to dial playground: %v", err)
		}
		return pb.NewPlaygroundServiceClient(cc)
	}
}

func init() {
	ctx := context.Background()

	repo := MakeRepo(ctx)
	pgClient = MakePlaygroundClient(ctx)
	svc = &service.Svc{Repo: repo, PgClient: pgClient}
	auth = MakeAuthorizer(ctx, repo)
	commonGet := Common(http.MethodGet)
	commonPost := Common(http.MethodPost)

	// functions framework
	functions.HTTP("getSdkList", commonGet(getSdkList))
	functions.HTTP("getContentTree", commonGet(ParseSdkParam(getContentTree)))
	functions.HTTP("getUnitContent", commonGet(ParseSdkParam(getUnitContent)))

	functions.HTTP("getUserProgress", commonGet(ParseSdkParam(auth.ParseAuthHeader(getUserProgress))))
	functions.HTTP("postUnitComplete", commonPost(ParseSdkParam(auth.ParseAuthHeader(postUnitComplete))))
	functions.HTTP("postUserCode", commonPost(ParseSdkParam(auth.ParseAuthHeader(postUserCode))))
	functions.HTTP("postDeleteProgress", commonPost(auth.ParseAuthHeader(postDeleteProgress)))
}

// Get list of SDK names
// Used in both representation and accessing content.
func getSdkList(w http.ResponseWriter, r *http.Request) {
	sdks := tob.MakeSdkList()

	err := json.NewEncoder(w).Encode(sdks)
	if err != nil {
		log.Println("Format sdk list error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "format sdk list")
		return
	}
}

// Get the content tree for a given SDK
// Required to be wrapped into ParseSdkParam middleware.
func getContentTree(w http.ResponseWriter, r *http.Request) {
	sdk := getContextSdk(r)
	tree, err := svc.GetContentTree(r.Context(), sdk)
	if err != nil {
		log.Println("Get content tree error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "storage error")
		return
	}

	err = json.NewEncoder(w).Encode(tree)
	if err != nil {
		log.Println("Format content tree error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "format content tree")
		return
	}
}

// Get unit content
// Everything needed to render a learning unit:
// description, hints, code snippets
// Required to be wrapped into ParseSdkParam middleware.
func getUnitContent(w http.ResponseWriter, r *http.Request) {
	sdk := getContextSdk(r)
	unitId := r.URL.Query().Get("id")

	unit, err := svc.GetUnitContent(r.Context(), sdk, unitId)
	if errors.Is(err, tob.ErrNoUnit) {
		finalizeErrResponse(w, http.StatusNotFound, NOT_FOUND, "unit not found")
		return
	}
	if err != nil {
		log.Println("Get unit content error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "storage error")
		return
	}

	err = json.NewEncoder(w).Encode(unit)
	if err != nil {
		log.Println("Format unit content error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "format unit content")
		return
	}
}

// Get user progress by sdk and uid
func getUserProgress(w http.ResponseWriter, r *http.Request) {
	sdk := getContextSdk(r)
	uid := getContextUid(r)

	progress, err := svc.GetUserProgress(r.Context(), sdk, uid)

	if err != nil {
		log.Println("Get user progress error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "storage error")
		return
	}

	err = json.NewEncoder(w).Encode(progress)
	if err != nil {
		log.Println("Format user progress error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "format user progress content")
		return
	}
}

// Mark unit completed
func postUnitComplete(w http.ResponseWriter, r *http.Request) {
	sdk := getContextSdk(r)
	uid := getContextUid(r)
	unitId := r.URL.Query().Get("id")

	err := svc.SetUnitComplete(r.Context(), sdk, unitId, uid)
	if errors.Is(err, tob.ErrNoUnit) {
		finalizeErrResponse(w, http.StatusNotFound, NOT_FOUND, "unit not found")
		return
	}
	if err != nil {
		log.Println("Set unit complete error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "storage error")
		return
	}

	fmt.Fprint(w, "{}")
}

// Save user code for unit
func postUserCode(w http.ResponseWriter, r *http.Request) {
	sdk := getContextSdk(r)
	uid := getContextUid(r)
	unitId := r.URL.Query().Get("id")

	var userCodeRequest tob.UserCodeRequest
	err := json.NewDecoder(r.Body).Decode(&userCodeRequest)
	if err != nil {
		log.Println("body decode error:", err)
		finalizeErrResponse(w, http.StatusBadRequest, BAD_FORMAT, "bad request body")
		return
	}

	err = svc.SaveUserCode(r.Context(), sdk, unitId, uid, userCodeRequest)
	if errors.Is(err, tob.ErrNoUnit) {
		finalizeErrResponse(w, http.StatusNotFound, NOT_FOUND, "unit not found")
		return
	}
	if err := errors.Unwrap(err); err != nil {
		log.Println("Save user code error:", err)
		message := "storage error"
		if st, ok := grpc_status.FromError(err); ok {
			message = fmt.Sprintf("playground api error: %s", st)
		}
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, message)
		return
	}

	fmt.Fprint(w, "{}")
}

// Delete user progress
func postDeleteProgress(w http.ResponseWriter, r *http.Request) {
	uid := getContextUid(r)

	err := svc.DeleteProgress(r.Context(), uid)
	if err != nil {
		log.Println("Delete progress error:", err)
		finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "storage error")
		return
	}

	fmt.Fprint(w, "{}")
}
