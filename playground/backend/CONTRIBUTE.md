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

# Contribution Guide

This guide consists of:

- [Project structure](#project-structure)
- [Generated Files](#generated-files)
- [Code processing pipeline](#code-processing-pipeline)
- [How to add a new supported language](#how-to-add-a-new-supported-language)

See also:
- [Database schema](SCHEMA.md)

## Project structure

```
backend/
├── cmd
│   ├── migration_tool           # tool to apply database migrations
│   ├── remove_unused_snippets   # tool to remove old snippets manually
│   └── server                   # entry point to the backend application
├── configs                      # config files for each SDK
├── containers                   # set up and build backend docker images
├── datasets                     # datasets for examples using Kafka emulator
├── internal                     # backend logic
│   ├── api                      # generated grpc API files
│   ├── cache                    # logic for working with cache
│   ├── code_processing          # logic for processing the received code
│   ├── components               # backend components
│   ├── constants                # code constants used in the application
│   ├── db                       # logic for working with database, e.g. the Cloud Datastore
│   ├── emulators                # logic for starting various emulators, e.g. Kafka
│   ├── environment              # tools for working with application environment settings
│   ├── errors                   # custom errors
│   ├── executors                # logic used to run the user submitted code
│   ├── external_functions       # logic for calling Google Cloud Functions
│   ├── fs_tool                  # logic for woking with filesystem operations during run preparation
│   ├── logger                   # cusotm logger
│   ├── preparers                # logic for preparing the user submitted code before execution
│   ├── setup_tools              # logic for setting up executors
│   ├── streaming                # implementation of run output streamer
│   ├── tasks                    # periodic tasks scheduler
│   ├── tests                    # common testing logic
│   ├── utils                    # miscellaneous tools
│   └── validators               # logic for pre-execution code validation
├── playground_functions         # Google Cloud Functions for write access to the database
├── functions.go                 # entry point for Cloud Functions
├── go.mod                       # Go project build configuration
├── logging.properties           # configuration for Java runner logger
├── new_scio_project.sh          # script for creating new SCIO project, used by SCIO runner
└── properties.yaml              # application properties
...
```

## Generated Files

All generated files (generated grpc API files, `go.sum`) should be published to the repository.

## Code processing pipeline

### Controller’s work

1. Backend receives a request for the `RunCode` API method
2. Backend checks that the SDK from the request matches the backend’s environment.
3. Backend generates the key of the code processing (`uuid` format), saves it to the cache, and sends it back to the
   client.
4. Backend starts a new goroutine that processes the code from the client request.

### Code processing goroutine

1. Backend sets up a timeout for each code processing.
2. Backend starts a new goroutine to check that current code processing is still actual and hasn’t been canceled by the
   client.
3. Validation of the received code.
4. Preparing the received code.
5. Compilation of the received code.
6. Execution of the received code.

Each step (`3-6` steps) is a separate goroutine and could be stopped if code processing has been canceled, or it takes
too much time.

After each step (even if it ends with failure) status of the code processing changes according to a finished step, so
the client clearly understands what is happening with the code processing at the moment.

Status, all outputs, and all error messages are placed to the one common cache, so even if there are several instances
it does not matter which instance process the code.

## How to add a new supported language

1. Add the language to [api.proto](../api/v1/api.proto) file:

```
enum Sdk {
  SDK_UNSPECIFIED = 0;
  SDK_JAVA = 1;
  SDK_GO = 2;
  SDK_PYTHON = 3;
  SDK_SCIO = 4;
}
```

2. Create a new environment for a new language as [this one](containers/java)
3. Create a new config file for a new language as [this one](configs/SDK_JAVA.json)
4. Update a method to create file system according to a new language [here](internal/fs_tool/fs.go) (`NewLifeCycle()`
   method)
5. Update a method to set up a file system according to a new
   language [here](internal/setup_tools/life_cycle/life_cycle_setuper.go) (`Setup()` method)
6. Update a method to set up code validator according to a new language[here](internal/utils/validators_utils.go)
7. Update a method to set up code preparers according to a new language [here](internal/utils/preparators_utils.go)
8. Update a method to set up compiler according to a new
   language [here](internal/setup_tools/builder/setup_builder.go) (`Compiler()` method)
9. Update a method to set up runner according to a new
   language [here](internal/setup_tools/builder/setup_builder.go) (`Runner()` method)
10. Update a method to set up test runner according to a new
    language [here](internal/setup_tools/builder/setup_builder.go) (`TestRunner()` method)
11. Update a method to compile client's code according to a new
    language [here](internal/code_processing/code_processing.go) (`compileStep()` method)
12. Update a method to execute client's code according to a new
    language [here](internal/code_processing/code_processing.go) (`runStep()` method)

## Adding an emulator-enabled example
1. Develop an example with an appropriate dataset
2. Put the dataset here in json or avro format as an array with objects: `playground/backend/datasets`
3. Put the example to the Apache Beam repository
4. Add a beam-playground comment to the example:
```yaml
 beam-playground:
   name: { example name }
   description: { description }
   multifile: { true | false }
   context_line: { the line where the code starts }
   categories:
     - { category }
   complexity: { BASIC | MEDIUM | ADVANCED }
   tags:
     - { tag }
   emulators:
     kafka:
        topic:
          id: { topic name }
          dataset: { dataset_1 }
   datasets:
     { dataset_1 }:
          location: local
          format: { json | avro }
```
5. Create a PR to the [Apache Beam Repository](https://github.com/apache/beam)
