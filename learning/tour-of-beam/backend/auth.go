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
	"strings"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	"beam.apache.org/learning/tour-of-beam/backend/internal/storage"
	firebase "firebase.google.com/go/v4"
)

// HandleFunc enriched with sdk and authenticated user uid.
type HandlerFuncAuthWithSdk func(w http.ResponseWriter, r *http.Request, sdk tob.Sdk, uid string)

const BEARER_SCHEMA = "Bearer "

type Authorizer struct {
	fbApp *firebase.App
	repo  storage.Iface
}

func MakeAuthorizer(ctx context.Context, repo storage.Iface) *Authorizer {
	// setup authorizer
	// consumes:
	// GOOGLE_PROJECT_ID
	// GOOGLE_APPLICATION_CREDENTIALS
	// OR
	// FIREBASE_AUTH_EMULATOR_HOST
	fbApp, err := firebase.NewApp(ctx, nil)
	if err != nil {
		log.Fatalf("error initializing firebase: %v", err)
	}
	return &Authorizer{fbApp, repo}
}

// middleware to parse authorization header, verify the ID token and extract uid.
func (a *Authorizer) ParseAuthHeader(next HandlerFuncAuthWithSdk) HandlerFuncWithSdk {
	return func(w http.ResponseWriter, r *http.Request, sdk tob.Sdk) {
		ctx := r.Context()
		header := r.Header.Get("authorization") // returns "" if no header
		if !strings.HasPrefix(header, BEARER_SCHEMA) {
			log.Printf("Bad authorization header")
			finalizeErrResponse(w, http.StatusUnauthorized, UNAUTHORIZED, "bad auth header")
			return
		}

		client, err := a.fbApp.Auth(ctx)
		if err != nil {
			log.Println("Failed to get auth client:", err)
			finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "auth client failed")
			return
		}

		tokenEncoded := header[len(BEARER_SCHEMA):]
		token, err := client.VerifyIDTokenAndCheckRevoked(ctx, tokenEncoded)
		if err != nil {
			log.Println("Failed to verify token:", err)
			finalizeErrResponse(w, http.StatusUnauthorized, UNAUTHORIZED, "failed to verify token")
			return
		}

		uid := token.UID
		// store in tb_user
		// TODO: implement IDToken caching in tb_user to optimize calls to Firebase API
		if err = a.repo.SaveUser(ctx, uid); err != nil {
			log.Println("Failed to store user info:", err)
			finalizeErrResponse(w, http.StatusInternalServerError, INTERNAL_ERROR, "failed to store user")
			return
		}

		next(w, r, sdk, uid)
	}
}
