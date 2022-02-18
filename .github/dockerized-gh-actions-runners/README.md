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
# GitHub Actions - Self-hosted Runners
The current GitHub Actions workflows are being tested on multiple operating systems, such as Ubuntu, Windows and MacOS. The way to migrate these runners from GitHub to GCP is by implementing self-hosted runners, so we have started implementing them in both Ubuntu and Windows environments, going with Google Kubernetes Engine and Google Cloud Compute VMs instances respectively.

In addition, we are working on researching the best way to implement the MacOS self-hosted runners.

## Ubuntu
Ubuntu Self-hosted runners are implemented using Google Kubernetes Engine with the following specifications:

#### Node
* Machine Type: 2-custom-6-18432
* Disk Size: 100 GB
* CPU: 6 vCPUs
* Memory : 18 GB

#### Pod
* Image: $LOCAL_IMAGE_NAME LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE:latest
* CPU: 2
* Memory: 1028 Mi
* Volumes: docker.sock
* Secret env variables: Kubernetes Secrets

#### AutoScaling
* Horizontal Pod Autoscaling
  * 5-10 nodes
  * HorizontalPodAutoscaler
    * Min replicas: 10 
    * Max replicas: 20 
    * CPU utilization: 70%
* Vertical Pod Autoscaling: updateMode: "Auto"


## Windows
Windows Virtual machines have the following specifications

#### VM specifications 
* Machine Type: n2-standard-2
* Disk Size: 70 GB 
* CPU: 2 vCPUs
* Memory : 8 GB

#### Instance group settings
* Region: us-west1 (multizone)
* Scale-out metric: 70% of CPU Usage.
* Cooldown period: 300s

#### Notes:
At first glance we considered to implement Windows runners using K8s, however this was not optimal because of the following reasons:

* VS Build tools are required for certain workflows, unfortunately official images that support this dependency are huge in size, reaching 20GB easily which is not an ideal case for k8S management.
* Windows Subsystem For Linux(WSL) is a feature that allows to execute bash scripts inside Windows which removes tech debt by avoiding writing steps in powershell, but this feature is disabled with payload removed in Windows containers.


## Self-Hosted Runners Architecture
![Diagram](diagrams/self-hosted-runners-architecture.png)

## Cronjob - Delete Unused Self-hosted Runners

Depending on the termination event, sometimes the removal script for offline runners is not triggered correctly from inside the VMs or K8s pod, because of that an additional pipeline was created in order to clean up the list of GitHub runners in the group.

This was implemented using a Cloud function subscribed to a Pub/Sub Topic, the topic is triggered through a Cloud Scheduler that is executed once per day, the function consumes a Github API to delete offline self-hosted runners from the organization retrieving the token with it's service account to secrets manager.


![Delete Offline Self-hosted Runners](diagrams/self-hosted-runners-delete-function.png)