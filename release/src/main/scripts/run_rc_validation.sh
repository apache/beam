#!/bin/bash
#
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

RELEASE=
REPO_URL=
RC_NUM=
RELEASE_BRANCH=
WORKING_BRANCH=
LOCAL_CLONE_DIR=rc_validations
BEAM_ROOT_DIR=beam
GIT_REPO_URL=https://github.com/apache/beam.git
PYTHON_RC_DOWNLOAD_URL=https://dist.apache.org/repos/dist/dev/beam

echo "[Input Required] Please enter the release version: "
read RELEASE
RELEASE_BRANCH=release-${RELEASE}
WORKING_BRANCH=release-${RELEASE}-RC${RC_NUM}_validations
echo "[Input Required] Please enter the release candidate number(1-3): "
read RC_NUM
echo "[Input Required] Please copy the repo URL from the vote email sent out by Release Manager:"
echo "The URL should look like: https://repository.apache.org/content/repositories/orgapachebeam-${KEY}"
read REPO_URL

echo "====================Checking Environment Variables================="
echo "running validations on release ${RELEASE} RC${RC_NUM}."
echo "repo URL for this RC: ${REPO_URL}"
echo "[Confirmation Required] Do you confirm all infos above are correct? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "====================Cloning Beam Release Branch===================="
cd ~
mkdir ${LOCAL_CLONE_DIR}
cd ${LOCAL_CLONE_DIR}
git clone ${GIT_REPO_URL}
cd ${BEAM_ROOT_DIR}
git checkout ${RELEASE_BRANCH}
git checkout -b ${WORKING_BRANCH}

echo "====================Starting Java Quickstart======================="

echo "[Current task] Java quickstart with direct runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-direct-java:runQuickstartJavaDirect \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Apex local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-apex:runQuickstartJavaApex \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Flink local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-flink_2.11:runQuickstartJavaFlinkLocal \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Spark local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-spark:runQuickstartJavaSpark \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Dataflow runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[GCP Project Required] Please input your GCP project:"
  read USER_GCP_PROJECT
  echo "[GCP GCS Bucket Required] Please input your GCS bucket: "
  read USER_GCS_BUCKET
  ./gradlew :beam-runners-google-cloud-dataflow-java:runQuickstartJavaDataflow \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE} \
  -PgcpProject=${USER_GCP_PROJECT} \
  -PgcsBucket=${USER_GCS_BUCKET}
fi

echo "===================Starting Java Mobile Game====================="
echo "[Confirmation Required] This task asks for GCP resources."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[GCP Project Required] Please input your GCP project:"
  read USER_GCP_PROJECT
  MOBILE_GAME_DATASET=${USER}_${RELEASE}_java_validations
  MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-${RELEASE}-java-topic-1
  echo "Please review following GCP sources setup: "
  echo "Using GCP project: ${USER_GCP_PROJECT}"
  echo "Will create BigQuery dataset: ${MOBILE_GAME_DATASET}"
  echo "Will create Pubsub topic: ${MOBILE_GMAE_PUBSUB_TOPIC}"
  echo "[Confirmation Required] Do you want to run validations with configurations above? [y|N]"
  read confirmation
  if [[ $confirmation = "y" ]]; then
    echo "-----------------Setting Up Service Account------------------------"
    echo "Please go to GCP IAM console under your project(${USER_GCP_PROJECT})."
    echo "Create a service account as project owner, if you don't have one."
    echo "[Input Required] Please enter your service account email:"
    read USER_SERVICE_ACCOUNT_EMAIL
    SERVICE_ACCOUNT_KEY_JSON=${USER}_json_key.json
    gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_EMAIL}
    export GOOGLE_APPLICATION_CREDENTIALS=pwd/${SERVICE_ACCOUNT_KEY_JSON}

    echo "-------------------Creating BigQuery Dataset-----------------------"
    bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
    bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

    echo "----------------------Creating Pubsub Topic------------------------"
    gcloud alpha pubsub topics delete projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC}
    gcloud alpha pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

    echo "[Current task] Java mobile game validations: UserScore, HourlyTeamScore, Leaderboard"
    ./gradlew :beam-runners-google-cloud-dataflow-java:runMobileGamingJavaDataflow \
    -Prepourl=${REPO_URL} \
    -Pver= ${RELEASE} \
    -PgcpProject=${USER_GCP_PROJECT} \
    -PgcsBucket=gs://dataflow-samples/game/gaming_data1.csv \
    -PbqDataset=${MOBILE_GAME_DATASET} -PpubsubTopic=${MOBILE_GAME_PUBSUB_TOPIC}
  fi
fi

echo "==================Starting Python Quickstart and MobileGame==================="
echo "This task will create a PR against apache/beam, trigger a jenkins job to run:"
echo "1. Python quickstart validations(batch & streaming)"
echo "2. Python MobileGame validations(UserScore, HourlyTeamScore)"
echo "[Confirmation Required] Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[Input Required] Please enter your github repo URL forked from apache/beam:"
  read USER_REMOTE_URL
  WORKING_BRANCH=python_validatoin_pr
  git checkout -b ${WORKING_BRANCH}
  touch empty_file.txt
  git add empty_file.txt
  git commit -m "Add empty file in order to create PR"
  git push -f ${USER_REMOTE_URL}
  hub pull-request -b apache:${RELEASE_BRANCH} -h boyuanzz:${WORKING_BRANCH} -F- <<<"[DO NOT MERGE]Run Python RC Validation Tests


  Run Python ReleaseCandidate"

  echo "[NOTE] If there is no jenkins job started, please comment generated PR with: Run Python ReleaseCandidate"
fi

echo "==============Starting Python Leaderboard & GameStates Validations==============="
echo "This task asks for GCP resources. Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  cd ~/${LOCAL_CLONE_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE}/python/apache-beam-${RELEASE}.zip
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE}/python/apache-beam-${RELEASE}.zip.sha512

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache-beam-${RELEASE}.zip.sha512

  echo "----------------------------Building Python SDK-------------------------------"
  sudo apt-get install unzip
  unzip apache-beam-${RELEASE}.zip
  cd apache-beam-${RELEASE}
  python setup.py sdist

  echo "---------------------------Setting up virtualenv------------------------------"
  pip install --upgrade pip
  pip install --upgrade setuptools
  pip install --upgrade virtualenv
  virtualenv beam_env
  . beam_env/bin/activate

  echo "--------------------------Installing Python SDK-------------------------------"
  pip install dist/apache-beam-${RELEASE}.tar.gz
  pip install dist/apache-beam-${RELEASE}.tar.gz[gcp]

  echo "----------------------------Setting up GCP Sources----------------------------"
  echo "[GCP Project Required] Please input your GCP project:"
  read USER_GCP_PROJECT
  MOBILE_GAME_GCS_BUCKET=gs://${USER}_${RELEASE}_python_validations_bucket
  MOBILE_GAME_DATASET=${USER}_${RELEASE}_python_validations
  MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-${RELEASE}-python-topic-1
  gsutil mb -p ${USER_GCP_PROJECT} ${MOBILE_GAME_GCS_BUCKET}
  gcloud alpha pubsub topics delete projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC}
  gcloud alpha pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

  echo "-----------------------Setting Up Service Account-----------------------------"
  echo "Please go to GCP IAM console under your project(${USER_GCP_PROJECT})."
  echo "Create a service account as project owner, if you don't have one."
  echo "[Input Required] Please enter your service account email:"
  read USER_SERVICE_ACCOUNT_EMAIL
  SERVICE_ACCOUNT_KEY_JSON=${USER}_json_key.json
  gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_EMAIL}N
  export GOOGLE_APPLICATION_CREDENTIALS=pwd/${SERVICE_ACCOUNT_KEY_JSON}

  echo "-----------------------Setting up Shell Env Vars------------------------------"
  cp ~/.bashrc ~/.bashrc_backup
  echo "export USER_GCP_PROJECT=${USER_GCP_PROJECT}" >> ~/.bashrc
  echo "export MOBILE_GAME_DATASET=${MOBILE_GAME_DATASET}" >> ~/.bashrc
  echo "export MOBILE_GAME_PUBSUB_TOPIC=${MOBILE_GAME_PUBSUB_TOPIC}" >> ~/.bashrc
  echo "export MOBILE_GAME_GCS_BUCKET=${MOBILE_GAME_GCS_BUCKET}" >> ~/.bashrc
  echo "export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> ~/.bashrc

  echo "--------------------------Updating ~/.m2/settings.xml-------------------------"
  cd ~
  if [[ -z `ls -a ~ | grep ".m2"` ]]; then
    mkdir .m2
  fi
  cd .m2
  if [[ ! -z `ls -a ~/.m2/ | grep "settings.xml"` ]]; then
    mv settings.xml settings_backup.xml
  fi
  touch settings.xml
  echo "<settings>" >> settings.xml
  echo "  <profiles>" >> settings.xml
  echo "    <profile>" >> settings.xml
  echo "      <id>release-repo</id>" >> settings.xml
  echo "      <activation>" >> settings.xml
  echo "        <activeByDefault>true</activeByDefault>" >> settings.xml
  echo "      </activation>" >> settings.xml
  echo "      <repositories>" >> settings.xml
  echo "        <repository>" >> settings.xml
  echo "          <id>Release ${RELEASE} RC${RC_NUM}</id>" >> settings.xml
  echo "          <name>Release ${RELEASE} RC${RC_NUM}</name>" >> settings.xml
  echo "          <url>${REPO_URL}</url>" >> settings.xml
  echo "        </repository>" >> settings.xml
  echo "      </repositories>" >> settings.xml
  echo "    </profile>" >> settings.xml
  echo "  </profiles>" >> settings.xml
  echo "</settings>" >> settings.xml

  echo "----------------------Starting Pubsub Java Injector--------------------------"
  cd ~/${LOCAL_CLONE_DIR}
  mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=${RELEASE} \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false \
      -DarchetypeCatalog=internal

  cd word-count-beam
  echo "A new terminal will pop up and start a java top injector."
  gnome-terminal -x sh -c 'source ~/.bashrc; mvn compile exec:java \
  -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
  -Dexec.args="${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC} none"; \
  exec bash'

  cd ~/${LOCAL_CLONE_DIR}/apache-beam-${RELEASE}/

  echo "----------------Starting Leaderboard with DirectRunner-----------------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  echo "This is a streaming job. This task will be launched in a separate terminal."
  gnome-terminal -x sh -c 'python -m apache_beam.examples.complete.game.leader_board --project=${USER_GCP_PROJECT} --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} --dataset ${USER}_test; exec bash'

  echo "Please wait for at least 5 mins to let results get populated."
  echo "How to verify results:"
  echo "1. Check whether there is any error messages in the task running terminal."
  echo "2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has leader_board_users and leader_board_teams table."
  echo "3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
  bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_users
  echo "4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
  bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_teams

  echo "If you have verified all items listed above, please terminate the python job."

  echo "----------------Starting Leaderboard with DataflowRunner---------------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  echo "This is a streaming job. This task will be launched in a separate terminal."
  gnome-terminal -x sh -c \
  'python -m apache_beam.examples.complete.game.leader_board \
  --project=${USER_GCP_PROJECT} \
  --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
  --dataset ${MOBILE_GAME_DATASET} \
  --runner DataflowRunner \
  --temp_location=${MOBILE_GAME_GCS_BUCKET}/temp/ \
  --sdk_location dist/*; \
  exec bash'

  echo "Please wait for at least 10 mins to let Dataflow job be launched and results get populated."
  echo "How to verify results:"
  echo "1. Goto your Dataflow job console and check whether there is any error."
  echo "2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has leader_board_users and leader_board_teams table."
  echo "3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
  bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_users
  echo "4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
  bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_teams

  echo "If you have verified all items listed above, please terminate this job in Dataflow Console."
  echo "[Confirmation Required] Please enter done to proceed into next step"
  read confirmation

  FIXED_WINDOW_DURATION=15

  echo "------------------Starting GameStats with DirectRunner-----------------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

  echo "This is a streaming job. This task will be launched in a separate terminal."
  echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
  gnome-terminal -x sh -c \
  'python -m apache_beam.examples.complete.game.game_stats \
  --project=${USER_GCP_PROJECT} \
  --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
  --dataset ${MOBILE_GAME_DATASET} \
  --fixed_window_duration ${FIXED_WINDOW_DURATION}; \
  exec bash'

  echo "Please wait for at least 20 mins to let results get populated."
  echo "How to verify results:"
  echo "1. Check whether there is any error messages in the task running terminal."
  echo "2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has game_stats_teams and game_stats_sessions table."
  echo "3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
  bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_teams
  echo "4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
  bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_sessions

  echo "If you have verified all items listed above, please terminate the python job."
  echo "[Confirmation Required] Please enter done to proceed into next step"
  read confirmation

  echo "-------------------Starting GameStats with DataflowRunner--------------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  echo "This is a streaming job. This task will be launched in a separate terminal."
  echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
  gnome-terminal -x sh -c \
  'python -m apache_beam.examples.complete.game.game_stats \
  --project=${USER_GCP_PROJECT} \
  --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
  --dataset ${MOBILE_GAME_DATASET} \
  --runner DataflowRunner \
  --temp_location=${MOBILE_GAME_GCS_BUCKET}/temp/ \
  --sdk_location dist/* \
  --fixed_window_duration ${FIXED_WINDOW_DURATION}; exec bash'

  echo "Please wait for at least 30 mins to let results get populated."
  echo "How to verify results:"
  echo "1. Goto your Dataflow job console and check whether there is any error."
  echo "2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has game_stats_teams and game_stats_sessions table."
  echo "3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
  bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_teams
  echo "4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
  bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_sessions

  echo "If you have verified all items listed above, please terminate the python job."
  echo "[Confirmation Required] Please enter done to proceed into next step"
  read confirmation
fi
