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

# Beam metrics

The beam metrics dashboard contains postcommit test passing rates
and is currently hosted at http://35.226.225.164/.

The dashboard is running on the Google cloud project
apache-beam-jenkins using kubernetes.

To test locally:

```
virtualenv --python=/usr/bin/python3 ./env
. ./env/bin/activate
pip install -r requirements.txt
python webserver.py
```

To deploy a new version:
```
gcloud container clusters get-credentials metrics-staging --zone=us-central1-a --project=apache-beam-testing
# get the most recent tag (increment when deploying, like v4, v5, v6)
gcloud describe pods
docker build -t gcr.io/apache-beam-testing/metrics:v5 .
docker image push gcr.io/apache-beam-testing/metrics:v5
kubectl set image deployment/metrics metrics=gcr.io/apache-beam-testing/metrics:v5
```
