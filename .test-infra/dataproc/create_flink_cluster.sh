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
#    Runs init actions for Docker, Portability framework (Beam) and Flink cluster
#    and opens an SSH tunnel to connect with Flink easily and run Beam jobs.
#
#    Provide the following environment to run this script:
#
#    CLUSTER_NAME: Cluster name
#    GCS_BUCKET: GCS bucket url for Dataproc resources (init actions)
#    HARNESS_IMAGES_TO_PULL: Urls to SDK Harness' images to pull on dataproc workers (accepts 1 or more urls)
#    FLINK_DOWNLOAD_URL: Url to Flink .tar archive to be installed on the cluster
#    FLINK_NUM_WORKERS: Number of Flink workers
#    TASK_MANAGER_SLOTS: Number of Flink slots per worker
#    DETACHED_MODE: Detached mode: should the SSH tunnel run in detached mode?
#
#    Example usage:
#
#    CLUSTER_NAME=flink \
#    GCS_BUCKET=gs://<GCS_BUCKET>/flink \
#    HARNESS_IMAGES_TO_PULL='gcr.io/<IMAGE_REPOSITORY>/python:latest gcr.io/<IMAGE_REPOSITORY>/java:latest' \
#    FLINK_DOWNLOAD_URL=http://archive.apache.org/dist/flink/flink-1.7.0/flink-1.7.0-bin-hadoop28-scala_2.12.tgz \
#    FLINK_NUM_WORKERS=2 \
#    TASK_MANAGER_SLOTS=1 \
#    DETACHED_MODE=false \
#    ./create_flink_cluster.sh
#
set -Eeuxo pipefail

DATAPROC_VERSION=1.2

MASTER_NAME="$CLUSTER_NAME-m"

# GCS properties
INIT_ACTIONS_FOLDER_NAME="init-actions"
FLINK_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/flink.sh"
BEAM_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/beam.sh"
DOCKER_INIT="$GCS_BUCKET/$INIT_ACTIONS_FOLDER_NAME/docker.sh"

# Flink properties
FLINK_LOCAL_PORT=8081
TASK_MANAGER_MEM=10240
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
    application_masters[$i]=`echo $line | sed "s/.*$CLUSTER_NAME/$CLUSTER_NAME/" | sed "s/ .*//"`
    i=$((i+1))
  done <<< $(gcloud compute ssh yarn@$MASTER_NAME --command="yarn application -list" | grep "$CLUSTER_NAME")

  if [ $i != 1 ]; then
    echo "Multiple applications found. Make sure that only 1 application is running on the cluster."
    for app in ${application_ids[*]};
    do
      echo $app
    done

    echo "Execute 'gcloud compute ssh yarn@$MASTER_NAME --command=\"yarn application -kill <APP_NAME>\"' to kill the yarn application."
    exit 1
  fi

  YARN_APPLICATION_MASTER=${application_masters[0]}
  echo "Using Yarn Application master: $YARN_APPLICATION_MASTER"
}

function start_tunnel() {
  local job_server_config=`gcloud compute ssh yarn@$MASTER_NAME --command="curl -s \"http://$YARN_APPLICATION_MASTER/jobmanager/config\""`
  local key="jobmanager.rpc.port"
  local yarn_application_master_host=`echo $YARN_APPLICATION_MASTER | cut -d ":" -f1`
  local jobmanager_rpc_port=`echo $job_server_config | python -c "import sys, json; print [ e['value'] for e in json.load(sys.stdin) if e['key'] == u'$key'][0]"`

  local detached_mode_params=$([[ $DETACHED_MODE == "true" ]] && echo " -Nf >& /dev/null" || echo "")
  local tunnel_command="gcloud compute ssh yarn@${MASTER_NAME} -- -L ${FLINK_LOCAL_PORT}:${YARN_APPLICATION_MASTER} -L ${jobmanager_rpc_port}:${yarn_application_master_host}:${jobmanager_rpc_port} -D 1080 ${detached_mode_params}"

  eval $tunnel_command
}

function create_cluster() {
  local metadata="beam-images-to-pull=${HARNESS_IMAGES_TO_PULL},"
  metadata+="flink-snapshot-url=${FLINK_DOWNLOAD_URL},"
  metadata+="flink-start-yarn-session=true"


  local image_version=${DATAPROC_VERSION:=1.2}

  echo "Starting dataproc cluster. Dataproc version: $image_version"

  # Docker init action restarts yarn so we need to start yarn session after this restart happens.
  # This is why flink init action is invoked last.
  gcloud dataproc clusters create $CLUSTER_NAME --num-workers=$FLINK_NUM_WORKERS --initialization-actions $DOCKER_INIT,$BEAM_INIT,$FLINK_INIT --metadata "${metadata}", --image-version=$image_version
}

function main() {
  upload_init_actions
  create_cluster # Comment this line to use existing cluster.
  get_leader
  start_tunnel
}

main "$@"
