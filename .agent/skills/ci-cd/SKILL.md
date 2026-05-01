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
description: Debugs CI failures, analyzes workflow logs, configures test matrices, and troubleshoots flaky tests in Apache Beam's GitHub Actions CI/CD system. Use when debugging CI failures, understanding test workflows, re-running failed checks, or modifying CI configuration.
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

Naming convention: `beam_{PreCommit,PostCommit}_{Language}[_Variant].yml`

- **PreCommit**: `Java`, `Python`, `Go`, `RAT` (license), `Spotless` (formatting)
- **PostCommit**: full test suites, `ValidatesRunner_*`, `Examples_*`
- **Performance**: `LoadTests_*`, `PerformanceTests_*`

## Triggering Tests

### Automatic
- PRs trigger PreCommit tests
- Merges trigger PostCommit tests

### Re-running Specific Workflows
```bash
# Via GitHub CLI
gh workflow run beam_PreCommit_Java.yml --ref your-branch

# Or use trigger files per the workflows README
```
See [trigger files](https://github.com/apache/beam/blob/master/.github/workflows/README.md#running-workflows-manually) for the full trigger phrase catalog.

## Understanding Test Results

### Finding Logs
1. Go to PR → Checks tab
2. Click on failed workflow
3. Expand failed job
4. View step logs

### Common Failure Patterns

#### Debugging Workflow
1. **Check if flaky**: review the workflow's recent runs in the Actions tab for the same test
   ```bash
   gh run list --workflow=beam_PreCommit_Java.yml --limit=10
   ```
2. **If flaky**: re-run the workflow
   ```bash
   gh run rerun <run-id> --failed
   ```
3. **If consistent**: reproduce locally using the same command from the workflow's `run` step
   ```bash
   # Example: find the failing command in the workflow file
   grep -A5 'run:' .github/workflows/beam_PreCommit_Java.yml
   # Then run it locally
   ./gradlew :sdks:java:core:test --info 2>&1 | tail -50
   ```
4. **If timeout**: check test runtime with `--info` flag; increase timeout only if justified
5. **If resource exhaustion**: check GCP quotas in project settings
6. **Verify fix**: push and confirm the workflow passes in the PR checks tab

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

Reproduce CI commands locally by reading the workflow's `run` step:
```bash
# Java PreCommit equivalent
./gradlew :sdks:java:core:test --info

# Python PreCommit equivalent
./gradlew :sdks:python:test

# If build state is stale
rm -rf ~/.gradle/caches .gradle build && ./gradlew clean

# Verify Java version matches CI
java -version  # CI typically uses JDK 11
```

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
