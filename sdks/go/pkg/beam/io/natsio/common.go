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

// Package natsio contains transforms for interacting with NATS.
package natsio

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsFn struct {
	URI       string
	CredsFile string
	nc        *nats.Conn
	js        jetstream.JetStream
}

func (fn *natsFn) Setup() error {
	var opts []nats.Option
	if fn.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(fn.CredsFile))
	}

	conn, err := nats.Connect(fn.URI, opts...)
	if err != nil {
		return fmt.Errorf("error connecting to NATS: %v", err)
	}
	fn.nc = conn

	js, err := jetstream.New(fn.nc)
	if err != nil {
		return fmt.Errorf("error creating JetStream context: %v", err)
	}
	fn.js = js

	return nil
}

func (fn *natsFn) Teardown() {
	if fn.nc != nil {
		fn.nc.Close()
	}
}
