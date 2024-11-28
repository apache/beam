#!/usr/bin/env bash
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Provide the following environment to run this script:
#
#    GCLOUD_ZONE: Google cloud zone. Optional. Default: "us-central1-a"
#    DATAPROC_VERSION: Dataproc version. Optional. Default: 2.2
#    CLUSTER_NAME: Cluster name
#    GCS_BUCKET: GCS bucket url for Dataproc resources (init actions)
#    HARNESS_IMAGES_TO_PULL: Urls to SDK Harness' images to pull on dataproc workers (optional: 0, 1 or multiple urls for every harness image)
#    JOB_SERVER_IMAGE: Url to job server docker image to pull on dataproc master (optional)
#    ARTIFACTS_DIR: Url to bucket where artifacts will be stored for staging (optional)
#    FLINK_DOWNLOAD_URL: Url to Flink .tar archive to be installed on the cluster
#    HADOOP_DOWNLOAD_URL: Url to a pre-packaged Hadoop jar
#    FLINK_NUM_WORKERS: Number of Flink workers
#    FLINK_TASKMANAGER_SLOTS: Number of slots per Flink task manager
#    DETACHED_MODE: Detached mode: should the SSH tunnel run in detached mode?
#
#    Example usage:
#    CLUSTER_NAME=flink \
#    GCS_BUCKET=gs://<GCS_BUCKET>/flink \
#    HARNESS_IMAGES_TO_PULL='gcr.io/<IMAGE_REPOSITORY>/python:latest gcr.io/<IMAGE_REPOSITORY>/java:latest' \
#    JOB_SERVER_IMAGE=gcr.io/<IMAGE_REPOSITORY>/job-server-flink:latest \
#    ARTIFACTS_DIR=gs://<bucket-for-artifacts> \
#    FLINK_DOWNLOAD_URL=https://archive.apache.org/dist/flink/flink-1.17.0/flink-1.17.0-bin-scala_2.12.tgz \
#    HADOOP_DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar \
#    FLINK_NUM_WORKERS=2 \
#    FLINK_TASKMANAGER_SLOTS=1 \
#    DETACHED_MODE=false \
#    ./flink_cluster.sh create
#
set -Eeuxo pipefail

# GCloud properties
GCLOUD_ZONE="${GCLOUD_ZONE:=us-central1-a}"
DATAPROC_VERSION="${DATAPROC_VERSION:=2.2-debian}"
GCLOUD_REGION=`echo $GCLOUD_ZONE | sed -E "s/(-[a-z])?$//"`

MASTER_NAME="$CLUSTER_NAME-m"

# GCS properties
INIT_ACTIONS_FOLDER_NAME="init-actions"
FLINK_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/flink.sh"
BEAM_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/beam.sh"
DOCKER_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/docker.sh"

# Flink properties
FLINK_LOCAL_PORT=8081

# By default each taskmanager has one slot - use that value to avoid sharing SDK Harness by multiple tasks.
FLINK_TASKMANAGER_SLOTS="${FLINK_TASKMANAGER_SLOTS:=1}"

YARN_APPLICATION_MASTER=""

function upload_init_actions() {
  echo "Uploading initialization actions to GCS bucket: $GCS_BUCKET"
  gsutil cp -r $INIT_ACTIONS_FOLDER_NAME/* $GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME
}

function get_leader() {
  local i=0
  local application_ids
  local application_masters

  echo "Yarn Applications"
  while read line; do
    echo $line
    application_ids[$i]=`echo $line | sed "s/ .*//"`
    application_masters[$i]=`echo $line | sed -E "s#.*(https?://)##" | sed "s/ .*//"`
    i=$((i+1))
  done <<< $(gcloud compute ssh --zone=$GCLOUD_ZONE --quiet yarn@$MASTER_NAME --command="yarn application -list" | grep "Apache Flink")

  if [ $i != 1 ]; then
    echo "Multiple applications found. Make sure that only 1 application is running on the cluster."
    for app in ${application_ids[*]};
    do
      echo $app
    done

    echo "Execute 'gcloud compute ssh --zone=$GCLOUD_ZONE yarn@$MASTER_NAME --command=\"yarn application -kill <APP_NAME>\"' to kill the yarn application."
    exit 1
  fi

  YARN_APPLICATION_MASTER=${application_masters[0]}
  echo "Using Yarn Application master: $YARN_APPLICATION_MASTER"
}

function start_job_server() {
  gcloud compute ssh --zone=$GCLOUD_ZONE --quiet yarn@${MASTER_NAME} --command="sudo --user yarn docker run --detach --publish 8099:8099 --publish 8098:8098 --publish 8097:8097 --volume ~/.config/gcloud:/root/.config/gcloud ${JOB_SERVER_IMAGE} --flink-master=${YARN_APPLICATION_MASTER} --artifacts-dir=${ARTIFACTS_DIR}"
}

function start_tunnel() {
  local job_server_config=`gcloud compute ssh --quiet --zone=$GCLOUD_ZONE yarn@$MASTER_NAME --command="curl -s \"http://$YARN_APPLICATION_MASTER/jobmanager/config\""`
  local key="jobmanager.rpc.port"
  local yarn_application_master_host=`echo $YARN_APPLICATION_MASTER | cut -d ":" -f1`
  local jobmanager_rpc_port=`echo $job_server_config | python -c "import sys, json; print([e['value'] for e in json.load(sys.stdin) if e['key'] == u'$key'][0])"`

  local detached_mode_params=$([[ $DETACHED_MODE == "true" ]] && echo " -Nf >& /dev/null" || echo "")

  local job_server_ports_forwarding=$([[ -n "${JOB_SERVER_IMAGE:=}" ]] && echo "-L 8099:localhost:8099 -L 8098:localhost:8098 -L 8097:localhost:8097" || echo "")

  local tunnel_command="gcloud compute ssh --zone=$GCLOUD_ZONE --quiet yarn@${MASTER_NAME} -- -L ${FLINK_LOCAL_PORT}:${YARN_APPLICATION_MASTER} -L ${jobmanager_rpc_port}:${yarn_application_master_host}:${jobmanager_rpc_port} ${job_server_ports_forwarding} -D 1080 ${detached_mode_params}"

  eval $tunnel_command
}

function create_cluster() {
  local metadata="flink-snapshot-url=${FLINK_DOWNLOAD_URL},"
  metadata+="flink-start-yarn-session=true,"
  metadata+="flink-taskmanager-slots=${FLINK_TASKMANAGER_SLOTS},"
  metadata+="hadoop-jar-url=${HADOOP_DOWNLOAD_URL}"

  [[ -n "${HARNESS_IMAGES_TO_PULL:=}" ]] && metadata+=",beam-sdk-harness-images-to-pull=${HARNESS_IMAGES_TO_PULL}"
  [[ -n "${JOB_SERVER_IMAGE:=}" ]] && metadata+=",beam-job-server-image=${JOB_SERVER_IMAGE}"

  local image_version=$DATAPROC_VERSION
  echo "Starting dataproc cluster. Dataproc version: $image_version"

  local worker_machine_type="n1-standard-2" # Default worker type
  local master_machine_type="n1-standard-2" # Default master type

  if [[ -n "${HIGH_MEM_MACHINE:=}" ]]; then
      worker_machine_type="${HIGH_MEM_MACHINE}"
      master_machine_type="${HIGH_MEM_MACHINE}"
  fi

  # Docker init action restarts yarn so we need to start yarn session after this restart happens.
  # This is why flink init action is invoked last.
  # TODO(11/22/2024) remove --worker-machine-type and --master-machine-type once N2 CPUs quota relaxed
  # Dataproc 2.1 uses n2-standard-2 by default but there is N2 CPUs=24 quota limit for this project
  gcloud dataproc clusters create $CLUSTER_NAME --enable-component-gateway --region=$GCLOUD_REGION --num-workers=$FLINK_NUM_WORKERS --public-ip-address \
  --master-machine-type=${master_machine_type} --worker-machine-type=${worker_machine_type} --metadata "${metadata}", \
  --image-version=$image_version --zone=$GCLOUD_ZONE --optional-components=FLINK,DOCKER  --quiet
}

# Runs init actions for Docker, Portability framework (Beam) and Flink cluster
# and opens an SSH tunnel to connect with Flink easily and run Beam jobs.
function create() {
  upload_init_actions
  create_cluster
  get_leader
  [[ -n "${JOB_SERVER_IMAGE:=}" ]] && start_job_server
  start_tunnel
}

# Recreates a Flink cluster.
function restart() {
  delete
  create
}

# Deletes a Flink cluster.
function delete() {
  gcloud dataproc clusters delete $CLUSTER_NAME --region=$GCLOUD_REGION --quiet
}

"$@"
