# Local Jenkins Setup

Setting up a local Jenkins instance is useful for testing Jenkins job changes
without affecting the production instance. Using Docker makes the setup very
straightforward.

## Jenkins on Docker

### Requirements
If you haven't yet, [install Docker on your machine.](https://docs.docker.com/install/)


### Setup using provided scripts

**WARNING: Not for production use.**

*INFO: Changing admin credentials is advised.*

You can utilize scripts in this folder to build Docker container with Jenkins,
pre-installed plugin and some basic configuration.

```bash
fetchplugins.sh
docker build -t beamjenkins .
docker run -p 127.0.0.1:8080:8080 beamjenkins:latest
```
* fetchplugin.sh -- fetches list of plugins from
  [infra wiki list](https://cwiki.apache.org/confluence/display/INFRA/Jenkins+Plugin+Upgrades).
* docker build -- builds image with name beamjenkins based on dockerfile located
  in current folder.
* docker run -- creates and starts new container.
    * The `-p 127.0.0.1:8080:8080` parameter to `docker run` ensures that your
      Jenkins instance will only be available via localhost and not your machine
      hostname.
* Default credentials: admin:jenadmin (Suggested to change these in dockerfile.)

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

### Manual setup

When invoking Docker, you can map a local directory to persist state and keep
your Jenkins configuration even after machine reboot. //TODO This is planned to
change to explaining how to spin up data container.

```bash
JENKINS_HOME=$(readlink --canonicalize-missing ~/jenkins/homedir)

mkdir --parents ${JENKINS_HOME}
chmod --recursive 777 ${JENKINS_HOME}
docker run -p 127.0.0.1:8080:8080 -v ${JENKINS_HOME}:/var/jenkins_home jenkins/jenkins:lts
```

Running this command will bring up your Jenkins instance at
http://127.0.0.1:8080. And map jenkins_home of running container to JENKINS_HOME
on your OS file system.

You can setup your Jenkins instance to look like the Apache Beam Jenkins using
[these steps](#beam-config).

//TODO Change 777 permissions, to minimum required set.

#### Forking your Jenkins instance

Later you can fork a new Docker container by copying the contents of the mapped
directory and starting Jenkins from the new copy.

This might be useful in case if you want to test new plugins/configuration and
want to have a clean roll-back option.

```bash
JENKINS_NEWHOME=$(readlink --canonicalize-missing ~/jenkins/homedir_v2)

mkdir --parents ${JENKINS_NEWHOME}
cp -R ${JENKINS_HOME} ${JENKINS_NEWHOME}
JENKINS_HOME=${JENKINS_NEWHOME}

chmod --recursive 777 ${JENKINS_HOME}
docker run -p 127.0.0.1:8080:8080 -v ${JENKINS_HOME}:/var/jenkins_home jenkins/jenkins:lts
```
* The `-p 127.0.0.1:8080:8080` parameter to `docker run` ensures that your 
      Jenkins instance will only be available via localhost and not your machine
      hostname.

#### Running Beam Job DSL groovy scripts {#beam-config}

On Beam we configure Jenkins jobs via groovy job dsl scripts. If you want to run
those on your docker instance of jenkins, you will need to do some setup on top
of installing default plugins:

1.  Install following plugins
    1.  Environment Injector
    1.  GitHub pull request build (ghprb)
    1.  Groovy
    1.  Job DSL
    1.  Node and Label parameter
    1.  (Optional) CustomHistory: This will allow you to easily generate job
        diffs as you make changes.
1.  Add "beam" label to jenkins instance
    1.  Go to Manage Jenkins -> Configure System
    1.  Type "beam" under "Labels" field.
1.  Disable script security. This way you will not have to approve all the
    scripts.
    1.  Go to Manage Jenkins -> Configure Global Security
    1.  Unmark "Enable script security for Job DSL scripts"

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
