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
This folder contains files required to deploy the Beam Community metrics stack
on your local machine.

This includes
* Python scripts for ingesting data from sources (Jenkins, JIRA,
  GitHub)
* Postgres analytics database
* [Grafana](https://grafana.com) dashboarding UI

All components run within Docker containers. These are composed together via
docker-compose for local hosting, and Kubernetes for the production instance on
GCP.

## Local setup

Docker Compose is used to host the full metrics stack on your local machine.

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

After running these commands, you can access the services running on your local
machine:

* Grafana: http://localhost:3000
* Postgres DB: localhost:5432

If you're deploying for the first time on your machine, follow instructions on
how to manually [configure Grafana](#configuring-grafana).

Grafana and Postgres containers persist data to Docker volumes, which will be
restored on subsequent runs. To start from a clean state, you must also wipe out
these volumes. (List volumes via `docker volume ls`)

## Kubernetes setup

The cloud-hosted topology is composed via Kubernetes instead of docker-compose.
Follow the steps below to re-deploy the production setup.

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

## Configuring Grafana

When you deploy a new Grafana instance, there is some one-time setup:

1. Log-in at http://localhost:3000 with username `admin` and the value specified
  for `GF_SECURITY_ADMIN_PASSWORD` in
  [`docker-compose.yml`](docker-compose.yml).
1. Add Postgres as a data source:
    1. Click the 'Add data source' button.
    1. Fill out the following config:
        * **Name**: BeamPSQL
        * **Type**: PostgreSQL
        * **Host** beampostgresql:5432
        * **Database**: beam\_metrics
        * **User**: admin
        * **Password**: `POSTGRES_PASSWORD` in
          [`docker-compose.yml`](docker-compose.yml).
        * **SSL Mode**: Disable
1. Restore dashboards from config
    1. In the Grafana sidebar, hover over the plus (+) and select 'Import'
    1. Select 'Upload .json File', and select the first exported JSON dashboard
       file in [dashboards/](dashboards)
    1. Repeat for each of the remaining exported dashboards.

## Appendix

### Useful Kubernetes commands and hints
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

### Useful docker commands and hints
* Connect from one container to another
    * `curl <containername>:<port>`
* Remove all containers/images/volumes
```sh
sudo docker rm $(sudo docker ps -a -q)
sudo docker rmi $(sudo docker images -q)
sudo docker volume prune
```
