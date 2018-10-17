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
# BeamMonitoring
This folder contains files required to spin-up metrics dashboard for Beam.

## Utilized technologies
* [Grafana](https://grafana.com) as dashboarding engine.
* PostgreSQL as underlying DB.

Approach utilized is to fetch data from corresponding system: Jenkins/Jira/GithubArchives/etc, put it into PostreSQL and fetch it to show in Grafana.

## Local setup

Install docker
* install docker
    * https://docs.docker.com/install/#supported-platforms
* install docker-compose
    * https://docs.docker.com/compose/install/#install-compose

```sh
# Remove old docker
sudo apt-get remove docker docker-engine docker.io

# Install docker
sudo apt-get update
sudo apt-get install \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   stable"
sudo apt-get update
sudo apt-get install docker-ce

###################################################
# Commands below here required to spin up docker  #
# containers locally. Can be omitted for kuberctl #
# setup.                                          #
###################################################

# Install docker-compose
sudo curl -L https://github.com/docker/compose/releases/download/1.22.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose


# start docker service if it is not running already
sudo service docker start

# Build images required for spinning up docker-compose
# related containers.
docker-compose build

# Spinup docker-compose related containers.
docker-compose up
```

## Kubernetes setup

1. Configure gcloud & kubectl
  * https://cloud.google.com/kubernetes-engine/docs/quickstart
2. Configure PosgreSQL
    a. https://pantheon.corp.google.com/sql/instances?project=apache-beam-testing
    b. Check on this link to configure connection from kubernetes to postgresql: https://cloud.google.com/sql/docs/postgres/connect-kubernetes-engine
3. add secrets for grafana
    a. `kubectl create secret generic grafana-admin-pwd --from-literal=grafana_admin_password=<pwd>`
4. create persistent volume claims:
```sh
kubectl create -f beam-grafana-etcdata-persistentvolumeclaim.yaml
kubectl create -f beam-grafana-libdata-persistentvolumeclaim.yaml
kubectl create -f beam-grafana-logdata-persistentvolumeclaim.yaml
```
5. Build and publish sync containers
```sh
cd sync/jenkins
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjenkins:v1 .
docker push gcr.io/${PROJECT_ID}/beammetricssyncjenkins:v1
```
6. Create deployment `kubectl create -f beamgrafana-deploy.yaml`

## Kubernetes update
https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

```sh
# Build and publish sync containers
cd sync/jenkins
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjenkins:v1 .
docker push -t gcr.io/${PROJECT_ID}/beammetricssyncjenkins:v1

# If needed check current pod status
kubectl get pods
kubectl describe pod <pod_id>

# Update container image via one of the following.
## update image for container 
kubectl set image deployment/beamgrafana container=<new_image_name>
## or update deployemnt from yaml file 
kubectl replace -f beamgrafana-deploy.yaml
```


## Useful Kubernetes commands and hints
```sh
# Get pods
kubectl get pods

# Get detailed status
kubectl describe pod <pod_name>

# Get logs
kubectl log <PodName> <ContainerName>

# Set kubectl logging level: -v [1..10]
https://github.com/kubernetes/kubernetes/issues/35054
```

## Useful docker commands and hints
* Connect from one container to another
    * `curl <containername>:<port>`
* Remove all containers/images/volumes
```sh
sudo docker rm $(sudo docker ps -a -q)
sudo docker rmi $(sudo docker images -q)
sudo docker volume prune
```
