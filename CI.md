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

# Apache Beam

## CI Environment

Continuous Integration is important component of making Apache Beam robust and stable.

Our execution environment for CI is mainly the Jenkins which is available at 
[https://ci-beam.apache.org/](https://ci-beam.apache.org/). See 
[.test-infra/jenkins/README](.test-infra/jenkins/README.md)
for trigger phrase, status and link of all Jenkins jobs. See Apache Beam Developer Guide for 
[Jenkins Tips](https://cwiki.apache.org/confluence/display/BEAM/Jenkins+Tips).

An additional execution environment for CI is [GitHub Actions](https://github.com/features/actions). GitHub Actions
(GA) are very well integrated with GitHub code and Workflow and it has evolved fast in 2019/2020 to become
a fully-fledged CI environment, easy to use and develop for, so we decided to use it for building python source
distribution and wheels.

## GitHub Actions

### GitHub actions run types

The following GA CI Job runs are currently run for Apache Beam, and each of the runs have different
purpose and context.

#### Pull request run

Those runs are results of PR from the forks made by contributors. Most builds for Apache Beam fall
into this category. They are executed in the context of the "Fork", not main
Beam Code Repository which means that they have only "read" permission to all the GitHub resources
(container registry, code repository). This is necessary as the code in those PRs (including CI job
definition) might be modified by people who are not committers for the Apache Beam Code Repository.

The main purpose of those jobs is to check if PR builds cleanly, if the test run properly and if
the PR is ready to review and merge.

#### Direct Push/Merge Run

Those runs are results of direct pushes done by the committers or as result of merge of a Pull Request
by the committers. Those runs execute in the context of the Apache Beam Code Repository and have also
write permission for GitHub resources (container registry, code repository).
The main purpose for the run is to check if the code after merge still holds all the assertions - like
whether it still builds, all tests are green.

This is needed because some of the conflicting changes from multiple PRs might cause build and test failures
after merge even if they do not fail in isolation.

#### Scheduled runs

Those runs are results of (nightly) triggered job - only for `master` branch. The
main purpose of the job is to check if there was no impact of external dependency changes on the Apache
Beam code (for example transitive dependencies released that fail the build). Another reason for the nightly 
build is that the builds tags most recent master with `nightly-master`.

All runs consist of the same jobs, but the jobs behave slightly differently or they are skipped in different
run categories. Here is a summary of the run categories with regards of the jobs they are running.
Those jobs often have matrix run strategy which runs several different variations of the jobs
(with different platform type / Python version to run for example)

### Google Cloud Platform Credentials

Some of the jobs require variables stored as [GitHub Secrets](https://docs.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets)
to perform operations on Google Cloud Platform.
These variables are:
 * `GCP_PROJECT_ID` - ID of the Google Cloud project. For example: `apache-beam-testing`.
 * `GCP_REGION` - Region of the bucket and dataflow jobs. For example: `us-central1`.
 * `GCP_TESTING_BUCKET` - Name of the bucket where temporary files for Dataflow tests will be stored. For example: `beam-github-actions-tests`.
 * `GCP_SA_EMAIL` - Service account email address. This is usually of the format `<name>@<project-id>.iam.gserviceaccount.com`.
 * `GCP_SA_KEY` - Service account key. This key should be created and encoded as a Base64 string (eg. `cat my-key.json | base64` on macOS).

Service Account shall have following permissions ([IAM roles](https://cloud.google.com/iam/docs/understanding-roles)):
 * Storage Admin (roles/storage.admin)
 * Dataflow Admin (roles/dataflow.admin)

### Workflows

#### Build python source distribution and wheels - [build_wheels.yml](.github/workflows/build_wheels.yml)

| Job                                             | Description                                                                                                                                                                                                                                                        | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|-------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Check GCP variables                             | Checks that GCP variables are set. Jobs which required them depend on the output of this job.                                                                                                                                                                      | Yes              | Yes                   | Yes           | Yes/No                   |
| Build python source distribution                | Builds python source distribution and uploads it to artifacts. Artifacts from release branch are used in release process ([`build_release_candidate.sh`](release/src/main/scripts/build_release_candidate.sh))                                                     | Yes              | Yes                   | Yes           | -                        |
| Prepare GCS                                     | Clears target path on GCS if already exists.                                                                                                                                                                                                                       | -                | Yes                   | Yes           | Yes                      |
| Upload python source distribution to GCS bucket | Uploads python source distribution to GCS bucket for path unique for specific workflow run.                                                                                                                                                                        | -                | Yes                   | Yes           | Yes                      |
| Build python wheels on linux/macos/windows      | Builds python wheels on linux/macos/windows platform with usage of `cibuildwheel` and uploads it to artifacts. Artifacts from release branch are used in release process ( [ `build_release_candidate.sh` ](release/src/main/scripts/build_release_candidate.sh) ) | Yes              | Yes                   | Yes           | -                        |
| Upload python wheels to GCS bucket              | Uploads python wheels to GCS bucket for path unique for specific workflow run. Additionally uploads workflow run data.                                                                                                                                             | -                | Yes                   | Yes           | Yes                      |
| List files on Google Cloud Storage Bucket       | Lists files on GCS for verification purpose.                                                                                                                                                                                                                       | -                | Yes                   | Yes           | Yes                      |
| Tag repo nightly                                | Tag repo with `nightly-master` tag if build python source distribution and python wheels finished successfully.                                                                                                                                                    | -                | -                     | Yes           | -                        |

#### Python tests - [python_tests.yml](.github/workflows/python_tests.yml)

| Job                              | Description                                                                                                           | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Check GCP variables              | Checks that GCP variables are set. Jobs which required them depend on the output of this job.                         | Yes              | Yes                   | Yes           | Yes/No                   |
| Build python source distribution | Builds python source distribution and uploads it to artifacts. Artifacts are used in `Python Wordcount Dataflow` job. | -                | Yes                   | Yes           | Yes                      |
| Python Unit Tests                | Runs python unit tests.                                                                                               | Yes              | Yes                   | Yes           | -                        |
| Python Wordcount Direct Runner   | Runs python WordCount example with Direct Runner.                                                                     | Yes              | Yes                   | Yes           | -                        |
| Python Wordcount Dataflow        | Runs python WordCount example with DataFlow Runner.                                                                   | -                | Yes                   | Yes           | Yes                      |

### GitHub Action Tips

* If you introduce changes to the workflow it is possible that your changes will not be present in the check run triggered in Pull Request. 
In this case please attach link to the modified workflow run executed on your fork.
* Possible timeouts with macOS runner - existing issue: [(X) This check failed - sometimes happens on macOS runner #841](https://github.com/actions/virtual-environments/issues/841)
* [GitHub Actions Documentation](https://docs.github.com/en/actions)
