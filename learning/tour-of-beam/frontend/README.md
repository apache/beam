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

# Tour of Beam

These are the main sources of the Tour of Beam website.

# About

## Getting started

Running, debugging, and testing all require this first step that fetches
dependencies and generates code:

```bash
cd ../../../playground/frontend/playground_components
flutter pub get
cd ../../../learning/tour-of-beam/frontend
flutter pub get
```

### Code Generation

This project relies on generated code for some functionality:
deserializers, constants for asset files, Firebase configuration, etc.

All generated files are version-controlled, so after checkout the project is immediately runnable.
However, after changes you may need to re-run code generation:
`flutter pub run build_runner build`

Manual for re-configuring Firebase:
https://firebase.google.com/docs/flutter/setup?platform=web

### Run

The following command is used to build and serve the frontend app locally:

`$ flutter run -d chrome`

# Deployment

# Tests

Install ChromeDriver to run integration tests in a browser: https://docs.flutter.dev/testing/integration-tests#running-in-a-browser
Run integration tests:
flutter drive \
 --driver=test_driver/integration_test.dart \
 --target=integration_test/counter_test.dart \
 -d web-server

# Packages

`flutter pub get`

# Contribution guide

For checks: `./gradlew rat`
Exclusions for file checks can be added in the Tour of Beam section of this file: `beam/build.gradle.kts`

# Additional resources

# Troubleshooting
