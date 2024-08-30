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

# Overview

This folder contains Python scripts to create a Pub/Sub topic under
the GCP project `apache-beam-testing` and test the topic.
The created topic is `projects/apache-beam-testing/topics/Imagenet_openimage_50k_benchmark`.

# Create the topic `Imagenet_openimage_50k_benchmark`

- Create one VM to run `gcs_image_looper.py`.
  The VM `pubsub-test-do-not-delete` was already created under `apache-beam-testing`.
  Keep the script running to continuously publish data.
- You might run `gcloud auth application-default login` to get the auth.
- You might run `pip install google-cloud-core google-cloud-pubsub google-cloud-storage`.
- Must make `Imagenet_openimage_50k_benchmark` public by adding `allAuthenticatedUsers` to the Pub/Sub Subscriber role.

# Tes the topic by subscribing it

- Run `test_image_looper.py` to check whether you could get any data.
