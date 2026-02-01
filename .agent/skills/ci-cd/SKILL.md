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

name: ci-cd
description: Guides understanding and working with Apache Beam's CI/CD system using GitHub Actions. Use when debugging CI failures, understanding test workflows, or modifying CI configuration.
---

# CI/CD in Apache Beam

## Overview
Apache Beam uses GitHub Actions for CI/CD. Workflows are located in `.github/workflows/`.

## Workflow Types

### PreCommit Workflows
- Run on PRs and merges
- Validate code changes before merge
- Naming: `beam_PreCommit_*.yml`

### PostCommit Workflows
- Run after merge and on schedule
- More comprehensive testing
- Naming: `beam_PostCommit_*.yml`

### Scheduled Workflows
- Run nightly on master
- Check for external dependency impacts
- Tag master with `nightly-master`

## Key Workflows

### PreCommit
| Workflow | Description |
|----------|-------------|
| `beam_PreCommit_Java.yml` | Java build and tests |
| `beam_PreCommit_Python.yml` | Python tests |
| `beam_PreCommit_Go.yml` | Go tests |
| `beam_PreCommit_RAT.yml` | License header checks |
| `beam_PreCommit_Spotless.yml` | Code formatting |

### PostCommit - Java
| Workflow | Description |
|----------|-------------|
| `beam_PostCommit_Java.yml` | Full Java test suite |
| `beam_PostCommit_Java_ValidatesRunner_*.yml` | Runner validation tests |
| `beam_PostCommit_Java_Examples_*.yml` | Example pipeline tests |

### PostCommit - Python
| Workflow | Description |
|----------|-------------|
| `beam_PostCommit_Python.yml` | Full Python test suite |
| `beam_PostCommit_Python_ValidatesRunner_*.yml` | Runner validation |
| `beam_PostCommit_Python_Examples_*.yml` | Examples |

### Load & Performance Tests
| Workflow | Description |
|----------|-------------|
| `beam_LoadTests_*.yml` | Load testing |
| `beam_PerformanceTests_*.yml` | I/O performance |

## Triggering Tests

### Automatic
- PRs trigger PreCommit tests
- Merges trigger PostCommit tests

### Manual via PR Comment
```
retest this please
```

### Specific Test Suites
Use trigger phrases from [catalog](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md)

### Workflow Dispatch
Most workflows support manual triggering via GitHub UI.

## Understanding Test Results

### Finding Logs
1. Go to PR → Checks tab
2. Click on failed workflow
3. Expand failed job
4. View step logs

### Common Failure Patterns

#### Flaky Tests
- Random failures unrelated to change
- Solution: Comment `retest this please`

#### Timeout
- Increase timeout in workflow if justified
- Or optimize test

#### Resource Exhaustion
- GCP quota issues
- Check project settings

## GCP Credentials

Workflows requiring GCP access use these secrets:
- `GCP_PROJECT_ID` - Project ID (e.g., `apache-beam-testing`)
- `GCP_REGION` - Region (e.g., `us-central1`)
- `GCP_TESTING_BUCKET` - Temp storage bucket
- `GCP_PYTHON_WHEELS_BUCKET` - Python wheels bucket
- `GCP_SA_EMAIL` - Service account email
- `GCP_SA_KEY` - Base64-encoded service account key

Required IAM roles:
- Storage Admin
- Dataflow Admin
- Artifact Registry Writer
- BigQuery Data Editor
- Service Account User

## Self-hosted vs GitHub-hosted Runners

### Self-hosted (majority of workflows)
- Pre-configured with dependencies
- GCP credentials pre-configured
- Naming: `beam_*.yml`

### GitHub-hosted
- Used for cross-platform testing (Linux, macOS, Windows)
- May need explicit credential setup

## Workflow Structure

```yaml
name: Workflow Name
on:
  push:
    branches: [master]
  pull_request:
    branches: [master]
  schedule:
    - cron: '0 0 * * *'
  workflow_dispatch:

jobs:
  build:
    runs-on: [self-hosted, ...]
    steps:
      - uses: actions/checkout@v4
      - name: Run Gradle
        run: ./gradlew :task:name
```

## Local Debugging

### Run Same Commands as CI
Check workflow file's `run` commands:
```bash
./gradlew :sdks:java:core:test
./gradlew :sdks:python:test
```

### Common Issues
- Clean gradle cache: `rm -rf ~/.gradle .gradle`
- Remove build directory: `rm -rf build`
- Check Java version matches CI

## Snapshot Builds

### Locations
- Java SDK: https://repository.apache.org/content/groups/snapshots/org/apache/beam/
- SDK Containers: https://gcr.io/apache-beam-testing/beam-sdk
- Portable Runners: https://gcr.io/apache-beam-testing/beam_portability
- Python SDK: gs://beam-python-nightly-snapshots

## Release Workflows
| Workflow | Purpose |
|----------|---------|
| `cut_release_branch.yml` | Create release branch |
| `build_release_candidate.yml` | Build RC |
| `finalize_release.yml` | Finalize release |
| `publish_github_release_notes.yml` | Publish notes |
