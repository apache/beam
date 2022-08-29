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

package tob

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/service"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	"cloud.google.com/go/datastore"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var svc service.IContent

const (
	BAD_FORMAT     = "BAD_FORMAT"
	INTERNAL_ERROR = "INTERNAL_ERROR"
)

func init() {
	// dependencies
	// required:
	// * TOB_MOCK: respond with static samples
	// OR
	// * DATASTORE_PROJECT_ID: cloud project id
	// optional:
	// * DATASTORE_EMULATOR_HOST: emulator host/port (ex. 0.0.0.0:8888)
	if os.Getenv("TOB_MOCK") > "" {
		svc = &service.Mock{}
	} else {
		client, err := datastore.NewClient(context.Background(), "")
		if err != nil {
			log.Fatalf("new datastore client: %v", err)
		}
		svc = &service.Svc{Repo: &storage.DatastoreDb{Client: client}}
	}

	// functions framework
	functions.HTTP("sdkList", sdkList)
	functions.HTTP("getContentTree", getContentTree)
}

func finalizeErrResponse(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	resp := tob.CodeMessage{Code: code, Message: message}
	_ = json.NewEncoder(w).Encode(resp)
}

func sdkList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	fmt.Fprint(w, `{"names": ["Java", "Python", "Go"]}`)
}

func getContentTree(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	sdkStr := r.URL.Query().Get("sdk")
	sdk := tob.ParseSdk(sdkStr)
	if sdk == tob.SDK_UNDEFINED {
		log.Printf("Bad sdk: %v", sdkStr)
		message := fmt.Sprintf("Sdk not in: %v", tob.SdksList())
		finalizeErrResponse(w, http.StatusBadRequest, BAD_FORMAT, message)
		return
	}

	tree, err := svc.GetContentTree(r.Context(), sdk, nil /*TODO userId*/)
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
