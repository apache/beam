<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->
# Manual build and push

First set a tag you want to use:
```
export RUNNER_IMAGE_TAG=some_tag
```
After which you run the build command:
```
docker build -t us-central1-docker.pkg.dev/apache-beam-testing/beam-github-actions/beam-arc-runner:$RUNNER_IMAGE_TAG -t us-central1-docker.pkg.dev/apache-beam-testing/beam-github-actions/beam-arc-runner:$(git rev-parse --short HEAD) .
```
This builds and tags the image with both the Git SHA and desired tag set.

Authenticate to the docker repository in GCP with:
```
gcloud auth configure-docker us-central1-docker.pkg.dev
```
docker push us-central1-docker.pkg.dev/apache-beam-testing/beam-github-actions/beam-arc-runner:$RUNNER_IMAGE_TAG
docker push us-central1-docker.pkg.dev/apache-beam-testing/beam-github-actions/beam-arc-runner:$(git rev-parse --short HEAD)
```