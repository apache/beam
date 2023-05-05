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

package constants

// Cloud Datastore constants
const (
	Namespace             = "Playground"
	DatastoreNamespaceKey = "DATASTORE_NAMESPACE"
	IDDelimiter           = "_"
	CloudPathDelimiter    = "/"
	UserSnippetOrigin     = "PG_USER"
	ExampleOrigin         = "PG_EXAMPLES"
	TbUserSnippetOrigin   = "TB_USER"

	SnippetKind  = "pg_snippets"
	SchemaKind   = "pg_schema_versions"
	SdkKind      = "pg_sdks"
	FileKind     = "pg_files"
	ExampleKind  = "pg_examples"
	PCObjectKind = "pg_pc_objects"
	DatasetKind  = "pg_datasets"

	PCOutputType = "OUTPUT"
	PCLogType    = "LOG"
	PCGraphType  = "GRAPH"

	EmulatorHostKey   = "DATASTORE_EMULATOR_HOST"
	EmulatorHostValue = "127.0.0.1:8888"
	EmulatorProjectId = "test"
)
