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

# Release Candidate (RC) Testing Guide

This guide is meant for anybody who is interested in testing Beam Release Candidates against downstream projects. Note
that one need not have any status on the Apache Beam project (eg. PMC Member, Committer) to vote; all are welcome.
Please subscribe to the [dev list](https://lists.apache.org/list.html?dev@beam.apache.org), and vote on the RC Vote email thread.


## RC Testing Objectives

The RC testing process aims to:

 - Test new release candidates against existing code bases utilizing Apache Beam, to ensure there are no unexpected behaviors downstream.
 - Incorporate a breadth of perspectives (including validation on multiple SDKs and multiple runners), before releasing a new version.
 - Allow Beam Contributors to dogfood their changes and verify that they work as intended.


## Beam Release process overview
- For a comprehensive overview on the Beam release process, please take a look at our [release guide](https://github.com/apache/beam/blob/master/contributor-docs/release-guide.md).
- Note that release candidate votes will be open for 72 hours after the voting email is sent.


## Ideas for Python SDK Validators

_Note: Do the following in a dev-like environment._
- If you are a Python SDK user that utilizes notebooks (eg. Jupyter Notebooks, or Colab Notebooks), change `pip install`
to point to the new RC (e.g. `pip install apache_beam[gcp]==2.52.0rc1`). Re-execute the workflow to ensure everything
works as intended.
- If your workflow utilizes [Dataflow Templates](https://github.com/GoogleCloudPlatform/DataflowTemplates), or another way of launching your job, modify your `requirements.txt` file, `setup.py` file, or `DockerFile` to point to the new Beam RC.
- _Tip_: Run your pipeline both against Direct Runner, and another runner of your choice by modifying your job's `PipelineOptions`.


## Ideas for Java SDK Validators
_Note: Do the following in a dev-like environment._
- If you are a Java SDK user that utilizes Maven or Gradle in your workflow, in your `pom.xml` file, modify the `beam.version` to reflect the newest RC, and modify the `<url>` tag under `<repository>`, to point to the repository given in the vote email. [Example Changes](https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/1090/files).


## Ideas for Go SDK Validators
_Note: Do the following in a dev-like environment._
- If you utilize the Go SDK, use `go-get` to use the desired RC, for example, `go get -d github.com/apache/beam/sdks/v2@v2.xx.0-RC1`
- Utilize the `--environment_config`, to point to the new release, for example, `--environment_config=apache/beam_go_sdk:2.xx.0rc1`


## After validation

- Reply to [dev list](https://lists.apache.org/list.html?dev@beam.apache.org) vote thread with your [vote](https://www.apache.org/foundation/voting.html) and an explanation of the use case you tested.
- [Optional]: If your use case can be well represented by a test, consider contributing a test to Beam! However, note that the of the value of validation is manual testing outside of the Beam CI workflow, so it is still recommended (and highly encouraged!) to validate these test cases in future releases.