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
# This script automates release candidate validation process.
#
# It reads configurations from script.config, checks environment settings and
# runs a list of validation pipelines against multiple runners one after
# another.
#
# NOTE:
#   1. Please set all variables in script.config before running this script.
#   2. Please babysit this script until first pipeline starts.

cd $(dirname $0)
. script.config


function clean_up(){
  echo ""
  echo "====================Final Steps===================="
  echo "-----------------Stopping Pubsub Java Injector-----------------"
  echo "Please stop java injector manually."
  echo "-----------------Signing up Spreadsheet-----------------"
  echo "Please open this spreadsheet: https://s.apache.org/beam-release-validation"
  echo "Please sign up your name in the tests you have ran."

  echo "-----------------Final Cleanup-----------------"
  if [[ -f ~/.m2/$BACKUP_M2 ]]; then
    rm ~/.m2/settings.xml
    cp ~/.m2/$BACKUP_M2 ~/.m2/settings.xml
    rm ~/.m2/$BACKUP_M2
    echo "* Restored ~/.m2/settings.xml"
  fi

  if [[ -f ~/$BACKUP_BASHRC ]]; then
    rm ~/.bashrc
    cp ~/$BACKUP_BASHRC ~/.bashrc
    rm ~/$BACKUP_BASHRC
    echo "* Restored ~/.bashrc"
  fi

  rm -rf ${LOCAL_BEAM_DIR}
  echo "* Deleted workspace ${LOCAL_BEAM_DIR}"
}
trap clean_up EXIT

setup_bashrc=0
function set_bashrc(){
    [[ $setup_bashrc -eq 0 ]] || return
    # [BEAM-4518]
    FIXED_WINDOW_DURATION=20
    cp ~/.bashrc ~/$BACKUP_BASHRC
    echo "export USER_GCP_PROJECT=${USER_GCP_PROJECT}" >> ~/.bashrc
    echo "export USER_GCS_BUCKET=${USER_GCS_BUCKET}" >> ~/.bashrc
    echo "export SHARED_PUBSUB_TOPIC=${SHARED_PUBSUB_TOPIC}" >> ~/.bashrc
    echo "export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> ~/.bashrc
    echo "export RELEASE_VER=${RELEASE_VER}" >> ~/.bashrc
    echo "export FIXED_WINDOW_DURATION=${FIXED_WINDOW_DURATION}" >> ~/.bashrc
    echo "export LOCAL_BEAM_DIR=${LOCAL_BEAM_DIR}" >> ~/.bashrc
    setup_bashrc=1
}

RC_TAG="v${RELEASE_VER}-RC${RC_NUM}"
RELEASE_BRANCH="releasev${RELEASE_VER}"
WORKING_BRANCH=v${RELEASE_VER}-RC${RC_NUM}_validations
GIT_REPO_URL=https://github.com/apache/beam.git
PYTHON_RC_DOWNLOAD_URL=https://dist.apache.org/repos/dist/dev/beam
HUB_VERSION=2.12.0
HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
BACKUP_BASHRC=.bashrc_backup_$(date +"%Y%m%d%H%M%S")
BACKUP_M2=settings_backup_$(date +"%Y%m%d%H%M%S").xml
declare -a PYTHON_VERSIONS_TO_VALIDATE=("python3.6" "python3.8")

echo ""
echo "====================Checking Environment & Variables================="
echo "PLEASE update RC_VALIDATE_CONFIGS in file script.config first."
echo ""
echo "running validations on release ${RELEASE_VER} RC${RC_NUM}."
echo "repo URL for this RC: ${REPO_URL}"
echo "using workspace: ${LOCAL_BEAM_DIR}"
echo "validate Python versions: "$(IFS=$' '; echo "${PYTHON_VERSIONS_TO_VALIDATE[*]}")
echo ""
echo "All environment and workflow configurations from RC_VALIDATE_CONFIGS:"
for i in "${RC_VALIDATE_CONFIGS[@]}"; do
  echo "$i = ${!i}"
done
echo "TODO(BEAM-13054): parts of this script launch background processes with gnome-terminal,"
echo "It may not work well over ssh or within a tmux session. Using 'ssh -Y' may help."
echo "[Confirmation Required] Would you like to proceed with current settings? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right configurations."
  exit
fi

echo "----------------- Checking git -----------------"
if [[ -z ${GITHUB_TOKEN} ]]; then
  echo "Error: A Github personal access token is required to perform git push "
  echo "under a newly cloned directory. Please manually create one from Github "
  echo "website with guide:"
  echo "https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line"
  echo "Note: This token can be reused in other release scripts."
  exit
else
  if [[ -d ${LOCAL_BEAM_DIR} ]]; then
    rm -rf ${LOCAL_BEAM_DIR}
  fi
  echo "* Creating local Beam workspace: ${LOCAL_BEAM_DIR}"
  mkdir -p ${LOCAL_BEAM_DIR}
  echo "* Cloning Beam repo"
  git clone --branch ${RC_TAG} ${GIT_REPO_URL} ${LOCAL_BEAM_DIR}
  cd ${LOCAL_BEAM_DIR}
  git checkout -b ${WORKING_BRANCH} ${RC_TAG} --quiet
  echo "* Setting up git config"
  # Set upstream repo url with access token included.
  USER_REPO_URL=https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/${GITHUB_USERNAME}/beam.git
  git remote add ${GITHUB_USERNAME} ${USER_REPO_URL}
  # For hub access Github API.
  export GITHUB_TOKEN=${GITHUB_TOKEN}
  # For local git repo only. Required if global configs are not set.
  git config user.name "${GITHUB_USERNAME}"
  git config user.email "${GITHUB_USERNAME}@gmail.com"
fi

echo "-----------------Checking hub-----------------"
if [[ -z `which hub` ]]; then
  if [[ "${INSTALL_HUB}" = true ]]; then
    echo "-----------------Installing hub-----------------"
    wget https://github.com/github/hub/releases/download/v${HUB_VERSION}/${HUB_ARTIFACTS_NAME}.tgz
    tar zvxvf ${HUB_ARTIFACTS_NAME}.tgz
    sudo ./${HUB_ARTIFACTS_NAME}/install
    echo "eval "$(hub alias -s)"" >> ~/.bashrc
    rm -rf ${HUB_ARTIFACTS_NAME}*
  else
    echo "Hub is not installed. Validation on Python Quickstart and MobileGame will be skipped."
  fi
fi
hub version

echo "-----------------Checking Google Cloud SDK-----------------"
if [[ -z `which gcloud` ]]; then
  if [[ "${INSTALL_GCLOUD}" = true ]]; then
    echo "-----------------Installing Google Cloud SDK-----------------"
    sudo apt-get install google-cloud-sdk

    gcloud init
    gcloud config set project ${USER_GCP_PROJECT}
    gcloud config set compute/region ${USER_GCP_REGION}

    echo "-----------------Setting Up Service Account-----------------"
    if [[ ! -z "${USER_SERVICE_ACCOUNT_EMAIL}" ]]; then
      SERVICE_ACCOUNT_KEY_JSON=~/google-cloud-sdk/${USER}_json_key.json
      gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_EMAIL}
      export GOOGLE_APPLICATION_CREDENTIALS=${SERVICE_ACCOUNT_KEY_JSON}
    else
      echo "Missing USER_SERVICE_ACCOUNT_EMAIL from config file. Force terminate."
      exit
    fi
  else
    echo "Google Cloud SDK is not installed."
  fi
fi
gcloud --version

echo "-----Initializing gcloud default and application-default credentials-----"
gcloud auth login
gcloud auth application-default login

echo "-----------------Checking gnome-terminal-----------------"
if [[ -z `which gnome-terminal` ]]; then
  echo "You don't have gnome-terminal installed."
  if [[ "$INSTALL_GNOME_TERMINAL" = true ]]; then
    sudo apt-get install gnome-terminal
  else
    echo "gnome-terminal is not installed. Can't run validation on Python Leaderboard & GameStates. Exiting."
    exit
  fi
fi
gnome-terminal --version

echo "-----------------Checking kubectl-----------------"
if [[ -z `which kubectl` ]]; then
  echo "You don't have kubectl installed."
  if [[ "$INSTALL_KUBECTL" = true ]]; then
    sudo apt-get install kubectl
  else
    echo "kubectl is not installed. Can't run validation on Python cross-language Kafka taxi. Exiting."
    exit
  fi
fi
kubectl version


echo ""
echo "====================Starting Python Quickstart and MobileGame==================="
echo "This task will create a PR against apache/beam, trigger a jenkins job to run:"
echo "1. Python quickstart validations(batch & streaming)"
echo "2. Python MobileGame validations(UserScore, HourlyTeamScore)"
if [[ "$python_quickstart_mobile_game" = true && ! -z `which hub` ]]; then
  touch empty_file.txt
  git add empty_file.txt
  git commit -m "Add empty file in order to create PR" --quiet
  git push -f ${GITHUB_USERNAME} --quiet
  # Create a test PR
  PR_URL=$(hub pull-request -b apache:${RELEASE_BRANCH} -h apache:${RC_TAG} -F- <<<"[DO NOT MERGE] Run Python RC Validation Tests

  Run Python ReleaseCandidate")
  echo "Created $PR_URL"
  # Comment on PR to trigger Python ReleaseCandidate Jenkins job.
  PR_NUM=$(echo $PR_URL | sed 's/.*apache\/beam\/pull\/\([0-9]*\).*/\1/')
  hub api repos/apache/beam/issues/$PR_NUM/comments --raw-field "body=Run Python ReleaseCandidate" > /dev/null
  echo ""
  echo "[NOTE] If there is no jenkins job started, please comment on $PR_URL with: Run Python ReleaseCandidate"
else
  echo "* Skipping Python Quickstart and MobileGame. Hub is required."
fi

# TODO(BEAM-13220) Run the remaining tests on Jenkins.
echo ""
echo "====================Starting Python Leaderboard & GameStates Validations==============="
if [[ ("$python_leaderboard_direct" = true \
      || "$python_leaderboard_dataflow" = true \
      || "$python_gamestats_direct" = true \
      || "$python_gamestats_dataflow" = true) \
      && ! -z `which gnome-terminal` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip.sha512
  if [[ ! -f apache-beam-${RELEASE_VER}.zip ]]; then
    { echo "Fail to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache-beam-${RELEASE_VER}.zip.sha512

  echo "--------------------------Updating ~/.m2/settings.xml-------------------------"
    cd ~
    if [[ ! -d .m2 ]]; then
      mkdir .m2
    fi
    cd .m2
    if [[ -f ~/.m2/settings.xml ]]; then
      mv settings.xml $BACKUP_M2
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
    echo "          <id>Release ${RELEASE_VER} RC${RC_NUM}</id>" >> settings.xml
    echo "          <name>Release ${RELEASE_VER} RC${RC_NUM}</name>" >> settings.xml
    echo "          <url>${REPO_URL}</url>" >> settings.xml
    echo "        </repository>" >> settings.xml
    echo "      </repositories>" >> settings.xml
    echo "    </profile>" >> settings.xml
    echo "  </profiles>" >> settings.xml
    echo "</settings>" >> settings.xml

  echo "-----------------------Setting up Shell Env Vars------------------------------"
    set_bashrc

  echo "----------------------Starting Pubsub Java Injector--------------------------"
  cd ${LOCAL_BEAM_DIR}
  mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=${RELEASE_VER} \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false \
      -DarchetypeCatalog=internal

  # Create a pubsub topic as a input source shared to all Python pipelines.
  SHARED_PUBSUB_TOPIC=leader_board-${USER}-python-topic-$(date +%m%d)_$RANDOM
  gcloud pubsub topics create --project=${USER_GCP_PROJECT} ${SHARED_PUBSUB_TOPIC}

  cd word-count-beam
  echo "A new terminal will pop up and start a java top injector."
  gnome-terminal -x sh -c \
  "echo '******************************************************';
   echo '* Running Pubsub Java Injector';
   echo '******************************************************';
  mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
  -Dexec.args='${USER_GCP_PROJECT} ${SHARED_PUBSUB_TOPIC} none';
  exec bash"

  # Run Leaderboard & GameStates pipelines under multiple versions of Python
  cd ${LOCAL_BEAM_DIR}
  for py_version in "${PYTHON_VERSIONS_TO_VALIDATE[@]}"
  do
    rm -rf ./beam_env_${py_version}
    echo "--------------Setting up virtualenv with $py_version interpreter----------------"
    $py_version -m venv beam_env_${py_version}
    . ./beam_env_${py_version}/bin/activate
    pip install --upgrade pip setuptools wheel

    echo "--------------------------Installing Python SDK-------------------------------"
    pip install apache-beam-${RELEASE_VER}.zip[gcp]

    echo "----------------Starting Leaderboard with DirectRunner-----------------------"
    if [[ "$python_leaderboard_direct" = true ]]; then
      LEADERBOARD_DIRECT_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project_id=${USER_GCP_PROJECT} ${LEADERBOARD_DIRECT_DATASET}
      echo "export LEADERBOARD_DIRECT_DATASET=${LEADERBOARD_DIRECT_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python Leaderboard with DirectRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.complete.game.leader_board \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${SHARED_PUBSUB_TOPIC} \
      --dataset ${LEADERBOARD_DIRECT_DATASET};
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 5 mins to let results get populated."
      echo "* Sleeping for 5 mins"
      sleep 5m
      echo "***************************************************************"
      echo "* How to verify results:"
      echo "* 1. Check whether there is any error messages in the task running terminal."
      echo "* 2. Goto your BigQuery console and check whether your ${LEADERBOARD_DIRECT_DATASET} has leader_board_users and leader_board_teams table."
      echo "* 3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
      bq head -n 10 ${LEADERBOARD_DIRECT_DATASET}.leader_board_users
      echo "* 4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
      bq head -n 10 ${LEADERBOARD_DIRECT_DATASET}.leader_board_teams
      echo "***************************************************************"
    else
      echo "* Skipping Python Leaderboard with DirectRunner"
    fi

    echo "----------------Starting Leaderboard with DataflowRunner---------------------"
    if [[ "$python_leaderboard_dataflow" = true ]]; then
      LEADERBOARD_DF_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project_id=${USER_GCP_PROJECT} ${LEADERBOARD_DF_DATASET}
      echo "export LEADERBOARD_DF_DATASET=${LEADERBOARD_DF_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python Leaderboard with DataflowRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.complete.game.leader_board \
      --project=${USER_GCP_PROJECT} \
      --region=${USER_GCP_REGION} \
      --topic projects/${USER_GCP_PROJECT}/topics/${SHARED_PUBSUB_TOPIC} \
      --dataset ${LEADERBOARD_DF_DATASET} \
      --runner DataflowRunner \
      --temp_location=${USER_GCS_BUCKET}/temp/ \
      --sdk_location apache-beam-${RELEASE_VER}.zip; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 10 mins to let Dataflow job be launched and results get populated."
      echo "* Sleeping for 10 mins"
      sleep 10m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Goto your BigQuery console and check whether your ${LEADERBOARD_DF_DATASET} has leader_board_users and leader_board_teams table."
      echo "* 3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
      bq head -n 10 ${LEADERBOARD_DF_DATASET}.leader_board_users
      echo "* 4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
      bq head -n 10 ${LEADERBOARD_DF_DATASET}.leader_board_teams
      echo "***************************************************************"
    else
      echo "* Skipping Python Leaderboard with DataflowRunner"
    fi

    echo "------------------Starting GameStats with DirectRunner-----------------------"
    if [[ "$python_gamestats_direct" = true ]]; then
      GAMESTATS_DIRECT_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project_id=${USER_GCP_PROJECT} ${GAMESTATS_DIRECT_DATASET}
      echo "export GAMESTATS_DIRECT_DATASET=${GAMESTATS_DIRECT_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running GameStats with DirectRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.complete.game.game_stats \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${SHARED_PUBSUB_TOPIC} \
      --dataset ${GAMESTATS_DIRECT_DATASET} \
      --fixed_window_duration ${FIXED_WINDOW_DURATION}; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 25 mins to let results get populated."
      echo "* Sleeping for 25mins"
      sleep 25m
      echo "* How to verify results:"
      echo "* 1. Check whether there is any error messages in the task running terminal."
      echo "* 2. Goto your BigQuery console and check whether your ${GAMESTATS_DIRECT_DATASET} has game_stats_teams and game_stats_sessions table."
      echo "* 3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
      bq head -n 10 ${GAMESTATS_DIRECT_DATASET}.game_stats_teams
      echo "* 4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
      bq head -n 10 ${GAMESTATS_DIRECT_DATASET}.game_stats_sessions
      echo "***************************************************************"
    else
      echo "* Skipping Python GameStats with DirectRunner"
    fi

    echo "-------------------Starting GameStats with DataflowRunner--------------------"
    if [[ "$python_gamestats_dataflow" = true ]]; then
      GAMESTATS_DF_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project_id=${USER_GCP_PROJECT} ${GAMESTATS_DF_DATASET}
      echo "export GAMESTATS_DF_DATASET=${GAMESTATS_DF_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running GameStats with DataflowRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.complete.game.game_stats \
      --project=${USER_GCP_PROJECT} \
      --region=${USER_GCP_REGION} \
      --topic projects/${USER_GCP_PROJECT}/topics/${SHARED_PUBSUB_TOPIC} \
      --dataset ${GAMESTATS_DF_DATASET} \
      --runner DataflowRunner \
      --temp_location=${USER_GCS_BUCKET}/temp/ \
      --sdk_location apache-beam-${RELEASE_VER}.zip \
      --fixed_window_duration ${FIXED_WINDOW_DURATION}; exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 30 mins to let results get populated."
      echo "* Sleeping for 30 mins"
      sleep 30m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Goto your BigQuery console and check whether your ${GAMESTATS_DF_DATASET} has game_stats_teams and game_stats_sessions table."
      echo "* 3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
      bq head -n 10 ${GAMESTATS_DF_DATASET}.game_stats_teams
      echo "* 4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
      bq head -n 10 ${GAMESTATS_DF_DATASET}.game_stats_sessions
      echo "***************************************************************"
    else
      echo "* Skipping Python GameStats with DataflowRunner"
    fi
  done # Loop over Python versions.
else
  echo "* Skipping Python Leaderboard & GameStates Validations"
fi

echo ""
echo "====================Starting Python Cross-language Validations==============="
if [[ ("$python_xlang_kafka_taxi_dataflow" = true
      || "$python_xlang_sql_taxi_dataflow" = true) \
      && ! -z `which gnome-terminal` && ! -z `which kubectl` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip.sha512
  if [[ ! -f apache-beam-${RELEASE_VER}.zip ]]; then
    { echo "Fail to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache-beam-${RELEASE_VER}.zip.sha512

  `which pip` install --upgrade pip
  `which pip` install --upgrade setuptools

  echo "-----------------------Setting up Shell Env Vars------------------------------"
  set_bashrc

  echo "-----------------------Setting up Kafka Cluster on GKE------------------------"
  CLUSTER_NAME=xlang-kafka-cluster-$RANDOM
  if [[ "$python_xlang_kafka_taxi_dataflow" = true ]]; then
    gcloud container clusters create --project=${USER_GCP_PROJECT} --region=${USER_GCP_REGION} --no-enable-ip-alias $CLUSTER_NAME
    kubectl apply -R -f ${LOCAL_BEAM_DIR}/.test-infra/kubernetes/kafka-cluster
    echo "* Please wait for 10 mins to let a Kafka cluster be launched on GKE."
    echo "* Sleeping for 10 mins"
    sleep 10m
  else
    echo "* Skipping Kafka cluster setup"
  fi

  # Run Python XLang pipelines under multiple versions of Python
  cd ${LOCAL_BEAM_DIR}
  for py_version in "${PYTHON_VERSIONS_TO_VALIDATE[@]}"
  do
    rm -rf ./beam_env_${py_version}
    echo "--------------Setting up virtualenv with $py_version interpreter----------------"
    $py_version -m venv beam_env_${py_version}
    . ./beam_env_${py_version}/bin/activate
    pip install --upgrade pip setuptools wheel
    ln -s ${LOCAL_BEAM_DIR}/sdks beam_env_${py_version}/lib/sdks

    echo "--------------------------Installing Python SDK-------------------------------"
    pip install apache-beam-${RELEASE_VER}.zip[gcp]

    echo "----------------Starting XLang Kafka Taxi with DataflowRunner---------------------"
    if [[ "$python_xlang_kafka_taxi_dataflow" = true ]]; then
      BOOTSTRAP_SERVERS="$(kubectl get svc outside-0 -o jsonpath='{.status.loadBalancer.ingress[0].ip}'):32400"
      echo "BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}"
      KAFKA_TAXI_DF_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      KAFKA_EXPANSION_SERVICE_JAR=${REPO_URL}/org/apache/beam/beam-sdks-java-io-expansion-service/${RELEASE_VER}/beam-sdks-java-io-expansion-service-${RELEASE_VER}.jar

      bq mk --project_id=${USER_GCP_PROJECT} ${KAFKA_TAXI_DF_DATASET}
      echo "export BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS}" >> ~/.bashrc
      echo "export KAFKA_TAXI_DF_DATASET=${KAFKA_TAXI_DF_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python XLang Kafka Taxi with DataflowRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.kafkataxi.kafka_taxi \
      --project=${USER_GCP_PROJECT} \
      --region=${USER_GCP_REGION} \
      --topic beam-runnerv2 \
      --bootstrap_servers ${BOOTSTRAP_SERVERS} \
      --bq_dataset ${KAFKA_TAXI_DF_DATASET} \
      --runner DataflowRunner \
      --num_workers 5 \
      --temp_location=${USER_GCS_BUCKET}/temp/ \
      --with_metadata \
      --beam_services=\"{\\\"sdks:java:io:expansion-service:shadowJar\\\": \\\"${KAFKA_EXPANSION_SERVICE_JAR}\\\"}\" \
      --sdk_location apache-beam-${RELEASE_VER}.zip; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 20 mins to let Dataflow job be launched and results get populated."
      echo "* Sleeping for 20 mins"
      sleep 20m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Check whether ${KAFKA_TAXI_DF_DATASET}.xlang_kafka_taxi has data, retrieving BigQuery data as below: "
      test_output=$(bq head -n 10 ${KAFKA_TAXI_DF_DATASET}.xlang_kafka_taxi)
      echo "$test_output"
      if ! grep -q "passenger_count" <<< "$test_output"; then
        echo "Couldn't find expected output. Please confirm the output by visiting the console manually."
        exit 1
      fi
      echo "***************************************************************"
    else
      echo "* Skipping Python XLang Kafka Taxi with DataflowRunner"
    fi

    echo "----------------Starting XLang SQL Taxi with DataflowRunner---------------------"
    if [[ "$python_xlang_sql_taxi_dataflow" = true ]]; then
      SQL_TAXI_TOPIC=${USER}_python_validations_$(date +%m%d)_$RANDOM
      SQL_TAXI_SUBSCRIPTION=${USER}_python_validations_$(date +%m%d)_$RANDOM
      SQL_EXPANSION_SERVICE_JAR=${REPO_URL}/org/apache/beam/beam-sdks-java-extensions-sql-expansion-service/${RELEASE_VER}/beam-sdks-java-extensions-sql-expansion-service-${RELEASE_VER}.jar

      gcloud pubsub topics create --project=${USER_GCP_PROJECT} ${SQL_TAXI_TOPIC}
      gcloud pubsub subscriptions create --project=${USER_GCP_PROJECT} --topic=${SQL_TAXI_TOPIC} ${SQL_TAXI_SUBSCRIPTION}
      echo "export SQL_TAXI_TOPIC=${SQL_TAXI_TOPIC}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '***************************************************';
       echo '* Running Python XLang SQL Taxi with DataflowRunner';
       echo '***************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.sql_taxi \
      --project=${USER_GCP_PROJECT} \
      --region=${USER_GCP_REGION} \
      --runner DataflowRunner \
      --num_workers 5 \
      --temp_location=${USER_GCS_BUCKET}/temp/ \
      --output_topic projects/${USER_GCP_PROJECT}/topics/${SQL_TAXI_TOPIC} \
      --beam_services=\"{\\\":sdks:java:extensions:sql:expansion-service:shadowJar\\\": \\\"${SQL_EXPANSION_SERVICE_JAR}\\\"}\" \
      --sdk_location apache-beam-${RELEASE_VER}.zip; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 20 mins to let Dataflow job be launched and results get populated."
      echo "* Sleeping for 20 mins"
      sleep 20m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Check whether your ${SQL_TAXI_SUBSCRIPTION} subscription has data below:"
      # run twice since the first execution would return 0 messages
      gcloud pubsub subscriptions pull --project=${USER_GCP_PROJECT} --limit=5 ${SQL_TAXI_SUBSCRIPTION}
      test_output=$(gcloud pubsub subscriptions pull --project=${USER_GCP_PROJECT} --limit=5 ${SQL_TAXI_SUBSCRIPTION})
      echo "$test_output"
      if ! grep -q "ride_status" <<< "$test_output"; then
        echo "Couldn't find expected output. Please confirm the output by visiting the console manually."
        exit 1
      fi
      echo "***************************************************************"
    else
      echo "* Skipping Python XLang SQL Taxi with DataflowRunner"
    fi
  done # Loop over Python versions.
else
  echo "* Skipping Python Cross-language Validations"
fi
echo "*************************************************************"
echo " NOTE: Streaming pipelines are not automatically canceled.   "
echo " Please manually cancel any remaining test pipelines and     "
echo " clean up the resources (BigQuery dataset, PubSub topics and "
echo " subscriptions, GKE cluster, etc.) after verification.       "
echo "*************************************************************"
