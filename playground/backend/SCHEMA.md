<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

- [Database](#database)
  - [Datastore namespaces](#datastore-namespaces)
  - [Migrations](#migrations)
  - [Datastore schema](#datastore-schema)
    - [pg\_schema\_versions](#pg_schema_versions)
    - [pg\_sdks](#pg_sdks)
    - [pg\_examples](#pg_examples)
    - [pg\_snippets](#pg_snippets)
      - [DatasetNestedEntity](#datasetnestedentity)
    - [pg\_datasets](#pg_datasets)
    - [pg\_files](#pg_files)
    - [pg\_pc\_objects](#pg_pc_objects)
  - [Datastore indexes](#datastore-indexes)
- [Redis](#redis)

# Database

Beam Playground uses Google Cloud Platform Datastore for storing examples and snippets. Redis is used for caching catalog reads from Datastore to avoid having to enumerate all of the exmaples on each catalog request.

## Datastore namespaces
Playground can use custom namespace to store entities in Datastore to support simultaneous deployment of several backend instances in the same GCP project. By default `Playground` namespace is used. custom namespace can be selected by setting `DATASTORE_NAMESPACE` environment variable.

## Migrations
If a breaking change to DB schema is made, it's adviced to implement a migration procedure to handle the data change in the Datastore. There are no formalized rules on when a migration should be implemented, but in general the following should be considered:
- all examples and precompiled objects are recreated during the deployment process and are not modified during the application runtime
- user code snippets do survive application update and may require data migration

In order to implement a migration a new Go file wtih name like `migration_xxx.go` should be created under the `internal/db/schema/migration` path. The file should contain a structure which implements the following interface:
```go
type Version interface {
	// GetVersion returns the version string of the schema
	GetVersion() string
	// GetDescription returns the description of the schema version
	GetDescription() string
	// InitiateData initializes the data for the schema or performs a migration
	InitiateData(args *DBArgs) error
}
```

After implementing the migration logic inside of the `InitiateData()` function it should be covered by tests and the new migration should be added to the list of exisitng migrations in the `cmd/server/server.go` file inside of `setupDBStructure()` function.

## Datastore schema
There are several entity kinds in Datastore:
| Entity kind | Description | Corresponding Go struct |
|-------------|-------------|-------------------------|
| [`pg_schema_versions`](#pg_schema_versions) | Schema version entity | `entity.SchemaEntity` |
| [`pg_sdks`](#pg_sdks) | SDK entity | `entity.SDKEntity` |
| [`pg_examples`](#pg_examples) | Example entity | `entity.ExampleEntity` |
| [`pg_snippets`](#pg_snippets) | Snippet entity | `entity.SnippetEntity` |
| [`pg_datasets`](#pg_datasets) | Dataset entity | `entity.DatasetEntity` |
| [`pg_files`](#pg_files) | File entity | `entity.FileEntity` |
| [`pg_pc_objects`](#pg_pc_objects) | Precompiled object entity | `entity.PrecompiledObjectEntity` |

### pg_schema_versions
This entity kind is used to store schema version and description of changes.
| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | Schema version | `Key` |
| `descr` | Description of changes | `string` |

### pg_sdks
This entity kind is used to store information about each supported SDK.

| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | SDK name | `Key` |
| `defaultExample` | Name of the default example for this SDK | `string` |

### pg_examples
This entity kind is used to store example catalog items.

| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | Example ID. Has form of `<SDK>_<Example name>` | `Key` |
| `name` | Example name | `string` |
| `sdk` | SDK ID | `Key` |
| `descr` | Example description | `string` |
| `tags` | Example tags | `[]string` |
| `cats` | Example categories | `[]string` |
| `path` | Url of the example on Github | `string` |
| `type` | Type of the example. Possible values are `PRECOMPILED_OBJECT_TYPE_UNSPECIFIED`, `PRECOMPILED_OBJECT_TYPE_EXAMPLE`, `PRECOMPILED_OBJECT_TYPE_KATA`, `PRECOMPILED_OBJECT_TYPE_UNIT_TEST` | `string` |
| `origin` | `PG_EXAMPLES` for Playground examples, `TB_EXAMPLES` for Tour of Beam examples | `string` |
| `schVer` | Schema version | `Key` |
| `urlVCS` | Url of the example on Github | `string` |
| `urlNotebook` | Url to a Collab notebook which has the example code | `string` |
| `alwaysRun` | If true, frontend will ignore any precompiled objects assosciated with the example and run it always | `bool` |

### pg_snippets
This entity kind is used to store snippets.

| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | Snippet ID. For shared user code the ID is computed based on the snippet content hash, for the snippets containing examples code the ID is the same as for the related example | `Key` |
| `ownerId` | ***Cannot find any usage***| `string` |
| `sdk` | SDK ID | `Key` |
| `pipeOpts` | Pipeline options | `string` |
| `created` | Creation time | `time.Time` |
| `lVisited` | Last visit time | `time.Time` |
| `origin` | `PG_SNIPPETS` for Playground examples, `TB_SNIPPETS` for Tour of Beam examples, `PG_USER` for snippets with code shared by users, `TB_USER` for snippets created by Tour Of Beam users | `string` |
| `visitCount` | Number of times the snippet was visited | `int` |
| `schVer` | Schema version | `Key` |
| `numberOfFiles` | Number of files in the snippet. Used to derive file keys. | `int` |
| `complexity` | Complexity of the snippet. Possible values are `COMPLEXITY_UNSPECIFIED`, `COMPLEXITY_BASIC`, `COMPLEXITY_MEDIUM`, `COMPLEXITY_ADVANCED` | `string` |
| `persistenceKey` | Used to track snippets created with Tour of Beam. When Tour of Beam user save a new snippet, all other snippets with the same persistenceKey are removed. | `string` |
| `datasets` | Contains an array of `DatasetNestedEntity` objects which describe datasets and emulators assosciated with the snippet | `[]DatasetNestedEntity` |

#### DatasetNestedEntity
| Field name | Description | Type |
|------------|-------------|------|
| `config` | A JSON serialized `map[string]string` object which contains emulator configuration | `string` |
| `dataset` | Dataset ID | `Key` |
| `emulator` | Emulator name. Currently only `kafka` is supported | `string` |

### pg_datasets
| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | Name of the dataset | Key |
| `path` | Path to the dataset file on the runner filesystem under path specified in `DATASETS_PATH` environment variable (`/opt/playground/backend/datasets` by default) | `string` |

### pg_files
| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | This field is constructed by concatenating snippet ID with an underscore (`_`) and an ordinal number of the file in the snippet. For example, if snippet `SDK_JAVA_Example` has `numberOfFiles` set to `2` then there will be two `pg_files` entities with `SDK_JAVA_Example_0` and `SDK_JAVA_Example_1` keys. | Key |
| `content` | Content of the file | `string` |
| `cntxLine` | Line number on which frontend will initially focus the text editor cursor when the file is being displayed | `int32` |
| `isMain` | Whether the file is the main file in the snippet. There can only be one main file in the snippet | `bool` |
| `name` | Name of the file which will shown to the user by the frontend | `string` |

### pg_pc_objects
These entities contain pre-compiled (cached) outputs of examples. There are three types of precompiled objects:
- `OUTPUT`, containing example's run output
- `LOG`, contianing example's log output
- `GRAPH`, containing example's execution graph output
All of these precompiled objects share the same schema

| Field name | Description | Type |
|------------|-------------|------|
| Name/ID | Key is constructed by concatenating example's ID with precompiled object type, e.g. `SDK_GO_WordCount_OUTPUT`, `SDK_GO_WordCount_LOG`, `SDK_GO_WordCount_GRAPH` | Key |
| `content` | Saved output of the example's run | `string` |

## Datastore indexes
Indexes are defined in [`index.yaml`](../index.yaml) file. The file is used during deployment to create indexes in the Datastore.

# Redis

Playground uses Redis as a cache for examples catalog to avoid having to re-enumerate all exmaples upon each request, as a temporary storage for examples output (logs, graphs, etc.) and as a message bus to relay events like a user request for pipeline cancellation.

Each pipeline run uses pipleine id as a Redis key, with the following subkeys ([source](internal/cache/cache.go)):
| Key | Subkey | Description |
|-----|--------|-------------|
| Pipeline Id | `STATUS` | Pipeline status. Possible values can be found in [`api.proto`](../api/v1/api.proto) in `Status` enum. |
| Pipeline Id | `RUN_OUTPUT` | Pipeline run output. |
| Pipeline Id | `RUN_ERROR` | Pipeline run error message. |
| Pipeline Id | `VALIDATION_OUTPUT` | Pipeline validation step output. |
| Pipeline Id | `PREPARATION_OUTPUT` | Pipeline preparation step output. |
| Pipeline Id | `COMPILE_OUTPUT` | Pipeline compilation step output. |
| Pipeline Id | `CANCELED` | Used to signal that user has requested pipeline cancellation. Runner periodically polls the cache to check if this key has been set to `true` and cancels the pipeline if it has. |
| Pipeline Id | `RUN_OUTPUT_INDEX` | Index of the start of the run step's output. Upon each request of the pipeline execution logs this value is set to the end of the returned log and used in subsequent requests to skip already sent log fragment. |
| Pipeline Id | `LOGS` | Pipeline execution logs. |

Additionally there are keys used globally by the Playground:
| Key | Subkey | Description |
|-----|--------|-------------|
| `EXAMPLES_CATALOG` | None | Used to store cached version of examples catalog. |
| `SDKS_CATALOG` | None | Used to store cached version of supported SDKS list with list of names of default examples. |
| `DEFAULT_PRECOMPILED_OBJECTS` | Sdk | Used to store a default example metadata in cache. |
