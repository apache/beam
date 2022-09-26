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
	"log"
	"net/http"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
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

// Helper common AIO middleware
func Common(next http.HandlerFunc) http.HandlerFunc {
	addContentType := AddHeader("Content-Type", "application/json")
	addCORS := AddHeader("Access-Control-Allow-Origin", "*")
	ensureGet := EnsureMethod(http.MethodGet)

	return ensureGet(addCORS(addContentType(next)))
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
			finalizeErrResponse(w, http.StatusBadRequest, BAD_FORMAT, "unknown sdk")
			return
		}

		next(w, r, sdk)
	}
}
