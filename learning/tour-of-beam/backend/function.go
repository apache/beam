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

const (
	BAD_FORMAT     = "BAD_FORMAT"
	INTERNAL_ERROR = "INTERNAL_ERROR"
	NOT_FOUND      = "NOT_FOUND"
)

// Middleware-maker for setting a header
// We also make this less generic: it works with HandlerFunc's
// so that to be convertible to func(w http ResponseWriter, r *http.Request)
// and be accepted by functions.HTTP.
func AddHeader(header, value string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add(header, value)
			next(w, r)
		}
	}
}

// Middleware to check http method.
func EnsureMethod(method string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method == method {
				next(w, r)
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		}
	}
}

// HandleFunc enriched with sdk.
type HandlerFuncWithSdk func(w http.ResponseWriter, r *http.Request, sdk tob.Sdk)

// middleware to parse sdk query param and pass it as additional handler param.
func ParseSdkParam(next HandlerFuncWithSdk) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sdkStr := r.URL.Query().Get("sdk")
		sdk := tob.ParseSdk(sdkStr)

		if sdk == tob.SDK_UNDEFINED {
			log.Printf("Bad sdk: %v", sdkStr)

			message := fmt.Sprintf("Sdk not in: %v", tob.SdksList())
			finalizeErrResponse(w, http.StatusBadRequest, BAD_FORMAT, message)

			return
		}

		next(w, r, sdk)
	}
}

// Helper to format http error messages.
func finalizeErrResponse(w http.ResponseWriter, status int, code, message string) {
	resp := tob.CodeMessage{Code: code, Message: message}

	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

var svc service.IContent

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
		// consumes DATASTORE_* env variables
		client, err := datastore.NewClient(context.Background(), "")
		if err != nil {
			log.Fatalf("new datastore client: %v", err)
		}
		svc = &service.Svc{Repo: &storage.DatastoreDb{Client: client}}
	}

	addHeader := AddHeader("Content-Type", "application/json")
	ensureGet := EnsureMethod(http.MethodGet)

	// functions framework
	functions.HTTP("sdkList", ensureGet(addHeader(sdkList)))
	functions.HTTP("getContentTree", ensureGet(addHeader(ParseSdkParam(getContentTree))))
	functions.HTTP("getUnitContent", ensureGet(addHeader(ParseSdkParam(getUnitContent))))
}

// Get list of SDK names
// Used in both representation and accessing content.
func sdkList(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, `{"names": ["Java", "Python", "Go"]}`)
}

// Get the content tree for a given SDK and user
// Merges info from the default tree and per-user information:
// user code snippets and progress
// Required to be wrapped into ParseSdkParam middleware.
func getContentTree(w http.ResponseWriter, r *http.Request, sdk tob.Sdk) {
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

// Get unit content
// Everything needed to render a learning unit:
// description, hints, code snippets
// Required to be wrapped into ParseSdkParam middleware.
func getUnitContent(w http.ResponseWriter, r *http.Request, sdk tob.Sdk) {
	unitId := r.URL.Query().Get("unitId")

	unit, err := svc.GetUnitContent(r.Context(), sdk, unitId, nil /*TODO userId*/)
	if err == service.ErrNoUnit {
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
