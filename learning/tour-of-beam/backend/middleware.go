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
	"log"
	"net/http"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const (
	BAD_FORMAT     = "BAD_FORMAT"
	INTERNAL_ERROR = "INTERNAL_ERROR"
	NOT_FOUND      = "NOT_FOUND"
	UNAUTHORIZED   = "UNAUTHORIZED"
)

// this subtypes here to pass go-staticcheck
type _ContextKeyTypeSdk string
type _ContextKeyTypeUid string

const (
	CONTEXT_KEY_SDK _ContextKeyTypeSdk = "sdk"
	CONTEXT_KEY_UID _ContextKeyTypeUid = "uid"
)

// helper to extract sdk from context
// set by ParseSdkParam middleware
// panics if key is not found
func getContextSdk(r *http.Request) tob.Sdk {
	return r.Context().Value(CONTEXT_KEY_SDK).(tob.Sdk)
}

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

// CORS handler, inspired by
// https://cloud.google.com/functions/docs/samples/functions-http-cors
// For more information about CORS and CORS preflight requests, see
// https://developer.mozilla.org/en-US/docs/Glossary/Preflight_request.
func AddCORS(methodAllow string) func(http.HandlerFunc) http.HandlerFunc {

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// Set CORS headers for the preflight request
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Methods", methodAllow)
				w.Header().Set("Access-Control-Allow-Headers", "*")
				w.Header().Set("Access-Control-Max-Age", "3600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// Set CORS headers for the main request.
			w.Header().Set("Access-Control-Allow-Origin", "*")

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
func Common(method string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		addContentType := AddHeader("Content-Type", "application/json")
		addCORS := AddCORS(method)
		ensureMethod := EnsureMethod(method)

		// addCORS handles OPTIONS, hence it is outside ensureMethod
		return addContentType(addCORS(ensureMethod(next)))
	}
}

// middleware to parse sdk query param and pass it as additional handler param.
func ParseSdkParam(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sdkStr := r.URL.Query().Get("sdk")
		sdk := tob.ParseSdk(sdkStr)

		if sdk == tob.SDK_UNDEFINED {
			log.Printf("Bad sdk: %v", sdkStr)
			finalizeErrResponse(w, http.StatusBadRequest, BAD_FORMAT, "unknown sdk")
			return
		}

		ctx := context.WithValue(r.Context(), CONTEXT_KEY_SDK, sdk)
		next(w, r.WithContext(ctx))
	}
}
