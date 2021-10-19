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

// Package sqlx contains "internal" SQL transform interfaces that are needed
// by the SQL expansion providers.
//
// The purposes of introducing a separate package from sql are:
// - to separate these "internal" APIs from the user-facing ones;
// - to break potential circular dependencies: sql -> default expansion service/handler -> sql.
package sqlx

const (
	Urn = "beam:external:java:sql:v1"

	// TODO: Change this to the Beam Java expansion address once Beam SQL
	// is implemented in Beam Go.
	DefaultExpansionAddr = "undefined"
)

// Option represents a custom SQL transform option. The option provider is
// responsible for marshaling and unmarshaling the option.
type Option struct {
	Urn     string `beam:"urn"`
	Payload []byte `beam:"payload"`
}

// ExpansionPayload is the struct of the payload encoded in ExpansionRequest.
type ExpansionPayload struct {
	Query   string   `beam:"query"`
	Dialect string   `beam:"dialect"`
	Options []Option `beam:"options"`
}
