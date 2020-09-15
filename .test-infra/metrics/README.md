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
This folder contains resources required to deploy the Beam metrics stack.

There are two types of metrics in Beam:
* Community metrics. The stack includes:
  * Python scripts for ingesting data from sources (Jenkins, JIRA,
  GitHub)
  * Postgres analytics database

* Test Results, i.e. metrics published by tests (IO Performance tests, Load tests and Nexmark tests). Beam uses InfluxDB time series database to store test metrics.


Both types of metrics are presented in [Grafana dashboard available here.](http://metrics.beam.apache.org)

Both stacks can be deployed on your local machine.

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
* InfluxDB: http://localhost:8086

If you're deploying for the first time on your machine, follow the wiki instructions
on how to manually [configure
Grafana](https://cwiki.apache.org/confluence/display/BEAM/Community+Metrics#CommunityMetrics-GrafanaUI).

Grafana, Postgres and InfluxDB containers persist data to Docker volumes, which will be
restored on subsequent runs. To start from a clean state, you must also wipe out
these volumes. (List volumes via `docker volume ls`)

## Kubernetes setup

Kubernetes deployment instructions are maintained in the wiki:
* [Community metrics](https://cwiki.apache.org/confluence/display/BEAM/Community+Metrics)
* [Test Results Monitoring](https://cwiki.apache.org/confluence/display/BEAM/Test+Results+Monitoring)
