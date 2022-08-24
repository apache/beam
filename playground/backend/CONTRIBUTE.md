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

## Project structure

```
backend/
├── cmd                   # set up and start backend application
├── configs               # config files for each SDK
├── containers            # set up and build backend images
├── internal              # backend business logic
│   ├── api                   # generated grpc API files
│   ├── cache                 # logic of work with cache
│   ├── code_processing       # logic of processing the received code
│   ├── components            # logic of work for more difficult processes using several packages
│   ├── constants             # application constants to use them anywhere in the application
│   ├── db                    # logic of work with database, e.g. the Cloud Datastore
│   ├── environment           # backend environments e.g. SDK of the instance
│   ├── errors                # custom errors to send them to the client
│   ├── executors             # logic of work with code executors
│   ├── fs_tool               # logic of work with a file system of code processing to prepare required files/folders
│   ├── logger                # custom logger
│   ├── preparers             # logic of preparing code before execution
│   ├── setup_tools           # logic of set up of executors and file systems by SDK requirements
│   ├── streaming             # logic of saving execution output as a stream
│   ├── tests                 # logic of work with unit/integration tests in the application, e.g. testing scripts to download mock data to database
│   ├── utils                 # different useful tools
│   └── validators            # logic of validation code before execution
├── go.mod                        # define backend go module and contain all project's dependencies
├── logging.properties            # config file to set up log for Java code
├── properties.yaml               # property file consists of application properties required for the operation logic
├── start_datastore_emulator.sh   # shell script to run the datastore emulator for local deployment or testing
├── stop_datastore_emulator.sh    # shell script to stop the datastore emulator
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