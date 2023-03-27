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

This project relies on generated code for some functionality:
deserializers, test mocks, constants for asset files,
extracted Beam symbols for the editor, etc.

All generated files are version-controlled, so after checkout the project is immediately runnable.
However, after changes you may need to re-run code generation:

```bash
cd beam
./gradlew :playground:frontend:playground_components:generateCode
cd learning/tour-of-beam/frontend
flutter pub run build_runner build --delete-conflicting-outputs
```

### Run

The following command is used to build and serve the frontend app locally:

```bash
flutter run -d chrome
```

### Backend Selection

To change the Google Project that is used as the backend:

1. Update Firebase configuration:
   https://firebase.google.com/docs/flutter/setup?platform=web

2. In `/lib/config.dart`, update:
   1. Google Project ID and region.
   2. Playground's backend URLs.

# Deployment

# Integration Tests

## Prerequisites

1. Install Google Chrome: https://www.google.com/chrome/
2. Install Chrome Driver: https://chromedriver.chromium.org/downloads
   - Note: This GitHub action installs both Chrome and Chrome Driver:
     https://github.com/nanasess/setup-chromedriver/blob/a249caaaad10fd12103028fd509853c2229eb6e6/lib/setup-chromedriver.sh
3. Retrieve the required dependencies for each project subdirectory by running the following commands:

```bash
cd playground/frontend/playground_components && flutter pub get && cd -
cd playground/frontend/playground_components_dev && flutter pub get && cd -
cd learning/tour-of-beam/frontend && flutter pub get && cd -
```

## Running Tests

1. Run the Chrome Driver on port 4444: `chromedriver --port=4444`
2. Run the integration tests:

```bash
# To run in a visible Chrome window:
./gradlew :learning:tour-of-beam:frontend:integrationTest

# Headless run without a browser window:
./gradlew :learning:tour-of-beam:frontend:integrationTest -PdeviceId=web-server
```


# Packages

`flutter pub get`

# Contribution guide

For checks: `./gradlew rat`
Exclusions for file checks can be added in the Tour of Beam section of this file: `beam/build.gradle.kts`

# Additional resources

# Troubleshooting
