---
title: "Building Apache Beam from Source"
type: "building"
layout: "arrow_template"
aliases:
  - /docs/building-beam/
  - /docs/build/
---

<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Building Apache Beam from Source

Apache Beam is an open source project that supports a variety of data processing models. To contribute to the project, test new features, or modify the code, you will need to build the project from source. This document guides you through the steps to pull the sources and build Beam locally.

### Prerequisites

Before you start building Apache Beam, make sure your environment is set up with the following prerequisites:

- **Java 8 or newer** – Apache Beam requires Java to build.
- **Maven** – The Apache Beam project uses Maven as the build system.
- **Git** – Git is used to clone the repository and track changes.
- **Docker** (Optional, for containerized build) – If you prefer to build using Docker, install Docker as well.

Ensure your system is updated and has the necessary resources to run a build.

### Step 1: Clone the Apache Beam Repository

Start by cloning the Apache Beam repository to your local machine:

1. Open a terminal and navigate to the directory where you want to store the project.
2. Run the following command to clone the Beam repository:

```bash
git clone https://github.com/apache/beam.git
cd beam
```

If you plan to contribute, fork the repository to your GitHub account first, then clone your fork instead:

```bash 
git clone https://github.com/<your-username>/beam.git
cd beam
git remote add upstream https://github.com/apache/beam.git
```
### Step 2: Installing Prerequisites

Apache Beam supports multiple languages. Install dependencies based on your language of choice.

### For Java

Install JDK 11 or later

Install Maven (if not already installed):
```bash
sudo apt install maven   # Ubuntu/Debian
brew install maven       # macOS
```
### For Python

Install Python 3.7 or later

Install dependencies:
```bash 
pip install -r sdks/python/requirements.txt
```

### Step 3: Building Apache Beam

Once you have installed the prerequisites and cloned the repository, you can proceed with building Apache Beam.

  ### Building the Entire Project

To build the full project, navigate to the root of the Beam repository and run:
```bash
 ./gradlew build
```
This command compiles all modules, runs unit tests, and packages the project.

### Building Without Tests

To speed up the build process, you can skip running the tests:

```bash
./gradlew build -x test
```

#### Building a Specific SDK

If you only need to build a particular SDK, navigate to the respective directory and run the build command.
For example:

  ###  Java SDK

```bash
./gradlew :sdks:java:core:build
```

  ### Python SDK

```bash
 cd sdks/python
 pip install . 
```

  ### Go SDK

```bash
 cd sdks/go
 go build ./...
```

### Step 4: Running Tests

To ensure everything works correctly, run the tests:

```bash
./gradlew test
```

To run tests for a specific SDK:

   ### Java

```bash
./gradlew :sdks:java:core:test
```

   ### Python

```bash
    cd sdks/python
    pytest apache_beam/
```

### Step 5: Keeping Your Repository Up to Date

To fetch the latest changes from the upstream Apache Beam repository:

``` bash
git fetch upstream
git checkout master
git merge upstream/master
```

For contributors working on feature branches, rebase your branch before submitting a PR:

```bash
git rebase upstream/master
```

### Step 6: Building with Docker (Optional)

For a containerized build environment, use Docker:

```bash
docker build -t apache-beam .
```

To run tests inside a container:

```bash
docker run -it apache-beam ./gradlew test
```

