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

# Local Jenkins Setup

Setting up a local Jenkins instance is useful for testing Jenkins job changes
without affecting the production instance. Using Docker makes the setup very
straightforward.

As of now, this setup allows you to test Job DSL scripts. **Imported Beam jobs
will not succeed due to some tools (like gcloud) and credentials missing.**

## Jenkins on Docker

### Requirements
If you haven't yet, [install Docker on your machine.](https://docs.docker.com/install/)

### Setup using provided scripts

**WARNING: Not for production use.**

*INFO: Changing admin credentials is advised.*

You can utilize scripts in this folder to build Docker container with Jenkins,
pre-installed plugins and some basic configuration.

```bash
docker build -t beamjenkins .
docker run -p 127.0.0.1:8080:8080 beamjenkins:latest
```
* plugins.txt -- Pre-assembled list of plugins that are available on Apache
  Jenkins. This list does not contain versions and might not have all the
  plugins available. If you find some plugin missing, or having and incorrect
  version, you can check
  [infra wiki list](https://cwiki.apache.org/confluence/display/INFRA/Jenkins+Plugin+Upgrades)
  for plugin updates history. Feel free to update this list with added/removed
  plugins.
* docker build -- builds image with name beamjenkins based on Dockerfile located
  in current folder.
* docker run -- creates and starts new container.
    * The `-p 127.0.0.1:8080:8080` parameter to `docker run` ensures that your
      Jenkins instance will only be available via localhost and not your machine
      hostname.
* Default credentials: admin:jenadmin (Suggested to change these in Dockerfile.)

Image built via this scripts will contain all required plugins, basic
configuration listed in manual setup instructions below and also remap
JENKINS_HOME, so it does not map to a volume, but is part of container instead.

This last action will allow you to persist changes you do to your container:

```bash
docker ps -a
docker commit <container_id> <new_image_name>
docker run -p 127.0.0.1:8080:8080 <new_image_name>
```
* docker ps -- list containers.
* docker commit -- creates new image based on container.

Such approach may be handy for debugging, but it is highly advised to keep all
data in docker volumes.

## Manual setup

When invoking Docker, you can map a local directory to persist state and keep
your Jenkins configuration even after machine reboot.

```bash
JENKINS_HOME=$(readlink --canonicalize-missing ~/jenkins/homedir)

mkdir -p ${JENKINS_HOME}
chmod -R 777 ${JENKINS_HOME}
docker run -p 127.0.0.1:8080:8080 -v ${JENKINS_HOME}:/var/jenkins_home jenkins/jenkins:lts
```

Running this command will bring up your Jenkins instance at
http://127.0.0.1:8080. And map jenkins_home of running container to JENKINS_HOME
on your OS file system.

You can setup your Jenkins instance to look like the Apache Beam Jenkins using
[these steps](#running-beam-job-dsl-groovy-scripts). (Running Beam Job DSL groovy scripts)

### Forking your Jenkins instance

Later you can fork a new Docker container by copying the contents of the mapped
directory and starting Jenkins from the new copy.

This might be useful in case if you want to test new plugins/configuration and
want to have a clean roll-back option.

```bash
JENKINS_NEWHOME=$(readlink --canonicalize-missing ~/jenkins/homedir_v2)

mkdir -p ${JENKINS_NEWHOME}
cp -R ${JENKINS_HOME} ${JENKINS_NEWHOME}
JENKINS_HOME=${JENKINS_NEWHOME}

chmod -R 777 ${JENKINS_HOME}
docker run -p 127.0.0.1:8080:8080 -v ${JENKINS_HOME}:/var/jenkins_home jenkins/jenkins:lts
```
* The `-p 127.0.0.1:8080:8080` parameter to `docker run` ensures that your
      Jenkins instance will only be available via localhost and not your machine
      hostname.

### Running Beam Job DSL groovy scripts

On Beam we configure Jenkins jobs via groovy job dsl scripts. If you want to run
those on your docker instance of Jenkins, you will need to do some setup on top
of installing default plugins:

1.  Make sure the following plugins are installed (Manage Jenkins -> Manage Plugins)
    1.  Environment Injector
    1.  GitHub pull request build (ghprb)
    1.  Groovy
    1.  Job DSL
    1.  Node and Label parameter
    1.  (Optional) CustomHistory: This will allow you to easily generate job
        diffs as you make changes.
1.  Add "beam" label to Jenkins instance
    1.  Go to Manage Jenkins -> Configure System
    1.  Type "beam" under "Labels" field.
1.  Disable script security. This way you will not have to approve all the
    scripts.
    1.  Go to Manage Jenkins -> Configure Global Security
    1.  Unmark "Enable script security for Job DSL scripts"
1.  Set up Job DSL jobs import job. (Seed job)
    1.  Refer to [seedjobconfig.xml](./seedjobconfig.xml) for parameters.
    1.  Go to Jenkins -> New Item -> Freestyle project
    1.  Build step: Process Job DSLs

## Additional Jenkins hints

### Importing DSL jobs from a local git repository

By default, Seed job imports DSL job definitions from the Apache Beam Github
repository. But there is also a possibility to import these definitions from 
your local git repository, which makes testing much easier because you don't 
have to git push every time changes were made. 

1. Build Jenkins image using provided scripts.
1. Provide an environment variable *BEAM_HOME* pointing to the beam root
   directory, for example: `export BEAM_HOME=~/my/beam/directory`.
1. Run image using the following command: `docker run -d -p 127.0.0.1:8080:8080
   -v $BEAM_HOME:/var/jenkins_real_home/beam:ro beamjenkins`. The only difference is
   the *-v* option which sets up a bind mount. 
1. Sign in to Jenkins.
    1. Go to the *sample_seed_job* and open its configuration. Scroll down to
       the **Source Code Management** section.
    1. Fill the **Repository URL** field with *file:///var/jenkins_real_home/beam*.

You can choose any branch from your local repo. Just remember that all changes
must be committed. You don’t have to checkout the branch you chose.

## Additional docker hints

### Running image vs starting container

When you execute `docker run`, it creates a new container for your image.
Instead of creating a new container every time, you can restart a
previously-exited container:

```bash
docker ps # Look for the previous container_id
docker start <container_id>
```

One of the benefits of restarting is that it will have all the data and
configuration parameters you have assigned to it upon creation.

### Delete all images and containers
```bash
docker rm $(docker ps -a -q)
docker rmi $(docker images -q)
docker volume prune
```
