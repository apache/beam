---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: gradle-build
description: Guides understanding and using the Gradle build system in Apache Beam. Use when building projects, understanding dependencies, or troubleshooting build issues.
---

# Gradle Build System in Apache Beam

## Overview
Apache Beam is a mono-repo using Gradle as its build system. The entire project (Java, Python, Go, website) is managed as a single Gradle project.

## Key Files
- `build.gradle.kts` - Root build configuration
- `settings.gradle.kts` - Project structure and module definitions
- `gradle.properties` - Global properties and versions
- `buildSrc/` - Custom Gradle plugins including BeamModulePlugin

## BeamModulePlugin
Located at `buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy`

### Purpose
- Manages Java dependencies centrally
- Configures project types (Java, Python, Go, Proto, Docker, etc.)
- Defines common custom tasks

### Java Project Configuration
```groovy
apply plugin: 'org.apache.beam.module'
applyJavaNature(
    automaticModuleName: 'org.apache.beam.sdk.io.kafka'
)
```

## Common Commands

### Build
```bash
# Build entire project
./gradlew build

# Build specific project
./gradlew :sdks:java:core:build
./gradlew -p sdks/java/core build

# Compile only (no tests)
./gradlew :sdks:java:core:compileJava
```

### Test
```bash
# Run tests
./gradlew :sdks:java:core:test

# Run specific test
./gradlew :sdks:java:core:test --tests *MyTest

# Skip tests
./gradlew build -x test
```

### Clean
```bash
# Clean specific project
./gradlew :sdks:java:core:clean

# Clean everything
./gradlew clean
```

### Formatting
```bash
# Java formatting (Spotless)
./gradlew spotlessApply

# Check formatting
./gradlew spotlessCheck

# Format CHANGES.md
./gradlew formatChanges
```

### Publishing
```bash
# Publish to Maven Local
./gradlew -Ppublishing :sdks:java:core:publishToMavenLocal

# Publish all Java artifacts
./gradlew -Ppublishing publishToMavenLocal
```

## Pre-commit Tasks

### Java
```bash
./gradlew javaPreCommit
```

### Python
```bash
./gradlew pythonPreCommit
```

### Combined
```bash
./gradlew :checkSetup  # Validates Go, Java, Python environments
```

## Useful Flags

| Flag | Description |
|------|-------------|
| `-p <path>` | Run task in specific project directory |
| `-x <task>` | Exclude task |
| `--tests <pattern>` | Filter tests |
| `-Ppublishing` | Enable publishing tasks |
| `-PdisableSpotlessCheck=true` | Disable formatting check |
| `-PdisableCheckStyle=true` | Disable checkstyle |
| `-PskipCheckerFramework` | Skip Checker Framework |
| `--continue` | Continue after failures |
| `--info` | Verbose output |
| `--debug` | Debug output |
| `--scan` | Generate build scan |
| `--parallel` | Parallel execution |

## GCP-related Properties

```bash
-PgcpProject=my-project
-PgcpRegion=us-central1
-PgcpTempRoot=gs://bucket/temp
-PgcsTempRoot=gs://bucket/temp
```

## Docker Tasks

```bash
# Build Java SDK container
./gradlew :sdks:java:container:java11:docker

# Build Python SDK container
./gradlew :sdks:python:container:py39:docker

# With custom repository
./gradlew :sdks:java:container:java11:docker \
  -Pdocker-repository-root=gcr.io/project \
  -Pdocker-tag=custom
```

## Dependency Management

### View Dependencies
```bash
./gradlew :sdks:java:core:dependencies
./gradlew :sdks:java:core:dependencies --configuration runtimeClasspath
```

### Force Dependency Version
In `build.gradle`:
```groovy
configurations.all {
    resolutionStrategy.force 'com.google.guava:guava:32.0.0-jre'
}
```

## Troubleshooting

### Clean Gradle Cache
```bash
rm -rf ~/.gradle/caches
rm -rf .gradle
rm -rf build
```

### Common Errors

#### NoClassDefFoundError
- Run `./gradlew clean`
- Delete gradle cache

#### Proto-related Errors
- Regenerate protos: `./gradlew generateProtos`

#### Dependency Conflicts
- Check dependencies: `./gradlew dependencies`
- Use `--scan` for detailed analysis

### Useful Tasks

```bash
# List all tasks
./gradlew tasks

# List tasks for a project
./gradlew :sdks:java:core:tasks

# Show project structure
./gradlew projects
```

## IDE Integration

### IntelliJ
1. Open repository root as Gradle project
2. Wait for indexing
3. Gradle tool window shows all tasks

### VS Code
Install Gradle extension for task discovery
