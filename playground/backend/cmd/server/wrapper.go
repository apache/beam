// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"net/http"
)

// Wrap a grpc.Server as a http.Handler
func Wrap(svc *grpc.Server, options []grpcweb.Option) http.Handler {
	wrappedServer := grpcweb.WrapServer(svc, options...)
	// TODO address what is the minimum necessary
	cors.AllowAll().Handler(&wrapper{
		svc: wrappedServer,
	})
	return wrappedServer
}

type wrapper struct {
	svc *grpcweb.WrappedGrpcServer
}

func (wrap *wrapper) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	wrap.svc.ServeHTTP(resp, req)
}
