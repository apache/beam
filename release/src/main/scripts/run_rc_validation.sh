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
  if [[ -f ~/.m2/settings_backup.xml ]]; then
    rm ~/.m2/settings.xml
    cp ~/.m2/settings_backup.xml ~/.m2/settings.xml
    echo "* Restored ~/.m2/settings.xml"
  fi

  if [[ -f ~/.bashrc_backup ]]; then
    rm ~/.bashrc
    cp ~/.bashrc_backup ~/.bashrc
    echo "* Restored ~/.bashrc"
  fi

  rm -rf ${LOCAL_BEAM_DIR}
  echo "* Deleted workspace ${LOCAL_BEAM_DIR}"
}
trap clean_up EXIT

RELEASE_BRANCH=release-${RELEASE_VER}
WORKING_BRANCH=release-${RELEASE}-RC${RC_NUM}_validations
GIT_REPO_URL=https://github.com/apache/beam.git
PYTHON_RC_DOWNLOAD_URL=https://dist.apache.org/repos/dist/dev/beam
HUB_VERSION=2.5.0
HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
declare -a PYTHON_VERSIONS_TO_VALIDATE=("python2.7" "python3.5")

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
echo "[Confirmation Required] Are they all provided and correctly set? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right configurations."
  exit
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

echo "-----------------Checking Bigquery CLI-----------------"
if [[ ! -f ~/.bigqueryrc ]]; then
  echo "-----------------Initialing Bigquery CLI-----------------"
  bq init
fi
bq version

echo "-----------------Checking gnome-terminal-----------------"
if [[ -z `which gnome-terminal` ]]; then
  echo "You don't have gnome-terminal installed."
  if [[ "$INSTALL_GNOME_TERMINAL" != true ]]; then
    sudo apt-get upgrade
    sudo apt-get install gnome-terminal
  else
    echo "gnome-terminal is not installed. Validation on Python Leaderboard & GameStates will be skipped."
    exit
  fi
fi
gnome-terminal --version


echo ""
echo ""
echo "====================Cloning Beam Release Branch===================="
if [[ -d ${LOCAL_BEAM_DIR} ]]; then
  rm -rf ${LOCAL_BEAM_DIR}
fi
echo "* Creating local Beam workspace: ${LOCAL_BEAM_DIR}"
mkdir -p ${LOCAL_BEAM_DIR}
git clone ${GIT_REPO_URL} ${LOCAL_BEAM_DIR}
cd ${LOCAL_BEAM_DIR}
git checkout -b ${WORKING_BRANCH} origin/${RELEASE_BRANCH}

echo ""
echo "====================Starting Java Quickstart======================="
echo "[Current task] Java quickstart with direct runner"
if [[ "$java_quickstart_direct" = true ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with DirectRunner"
  echo "*************************************************************"
  ./gradlew :runners:direct-java:runQuickstartJavaDirect \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER}
else
  echo "* Skip Java quickstart with direct runner"
fi

echo "[Current task] Java quickstart with Apex local runner"
if [[ "$java_quickstart_apex_local" = true ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Apex local runner"
  echo "*************************************************************"
  ./gradlew :runners:apex:runQuickstartJavaApex \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER}
else
  echo "* Skip Java quickstart with Apex local runner"
fi

echo "[Current task] Java quickstart with Flink local runner"
if [[ "$java_quickstart_flink_local" = true ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Flink local runner"
  echo "*************************************************************"
  ./gradlew :runners:flink:1.5:runQuickstartJavaFlinkLocal \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER}
else
  echo "* Skip Java quickstart with Flink local runner"
fi

echo "[Current task] Java quickstart with Spark local runner"
if [[ "$java_quickstart_spark_local" = true ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Spark local runner"
  echo "*************************************************************"
  ./gradlew :runners:spark:runQuickstartJavaSpark \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER}
else
  echo "* Skip Java quickstart with Spark local runner"
fi

echo "[Current task] Java quickstart with Dataflow runner"
if [[ "$java_quickstart_dataflow" = true && ! -z `which gcloud` ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with DataflowRunner"
  echo "*************************************************************"
  ./gradlew :runners:google-cloud-dataflow-java:runQuickstartJavaDataflow \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER} \
  -PgcpProject=${USER_GCP_PROJECT} \
  -PgcsBucket=${USER_GCS_BUCKET:5}  # skip 'gs://' prefix
else
  echo "* Skip Java quickstart with Dataflow runner. Google Cloud SDK is required."
fi

echo ""
echo "====================Starting Java Mobile Game====================="
if [[ "$java_mobile_game" = true && ! -z `which gcloud` ]]; then
  MOBILE_GAME_DATASET=${USER}_java_validations_$(date +%m%d)_$RANDOM
  MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-java-topic-$(date +%m%d)_$RANDOM
  echo "Using GCP project: ${USER_GCP_PROJECT}"
  echo "Will create BigQuery dataset: ${MOBILE_GAME_DATASET}"
  echo "Will create Pubsub topic: ${MOBILE_GAME_PUBSUB_TOPIC}"

  echo "-----------------Creating BigQuery Dataset-----------------"
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

  echo "-----------------Creating Pubsub Topic-----------------"
  gcloud pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

  echo "**************************************************************************"
  echo "* Java mobile game validations: UserScore, HourlyTeamScore, Leaderboard"
  echo "**************************************************************************"
  ./gradlew :runners:google-cloud-dataflow-java:runMobileGamingJavaDataflow \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE_VER} \
  -PgcpProject=${USER_GCP_PROJECT} \
  -PbqDataset=${MOBILE_GAME_DATASET} \
  -PpubsubTopic=${MOBILE_GAME_PUBSUB_TOPIC} \
  -PgcsBucket=${USER_GCS_BUCKET:5}  # skip 'gs://' prefix

  echo "-----------------Cleaning up BigQuery & Pubsub-----------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
  gcloud pubsub topics delete projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC}
else
  echo "* Skip Java Mobile Game. Google Cloud SDK is required"
fi

echo ""
echo "====================Starting Python Quickstart and MobileGame==================="
echo "This task will create a PR against apache/beam, trigger a jenkins job to run:"
echo "1. Python quickstart validations(batch & streaming)"
echo "2. Python MobileGame validations(UserScore, HourlyTeamScore)"
if [[ "$python_quickstart_mobile_game" = true && ! -z `which hub` ]]; then
  touch empty_file.txt
  git add empty_file.txt
  git commit -m "Add empty file in order to create PR"
  git push -f ${USER_REMOTE_URL}
  hub pull-request -b apache:${RELEASE_BRANCH} -h ${GITHUB_USERNAME}:${WORKING_BRANCH} -F- <<<"[DO NOT MERGE]Run Python RC Validation Tests


  Run Python ReleaseCandidate"

  echo "[NOTE] If there is no jenkins job started, please comment generated PR with: Run Python ReleaseCandidate"
else
  echo "* Skip Python Quickstart and MobileGame. Hub is required."
fi

echo ""
echo "====================Starting Python Leaderboard & GameStates Validations==============="
if [[ ("$python_leaderboard_direct" = true || \
      "$python_leaderboard_dataflow" = true || \
      "$python_gamestats_direct" = true || \
      "$python_gamestats_dataflow" = true) && \
      ! -z `which gnome-terminal` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache-beam-${RELEASE_VER}.zip.sha512

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache-beam-${RELEASE_VER}.zip.sha512

  `which pip` install --upgrade pip
  `which pip` install --upgrade setuptools
  `which pip` install --upgrade virtualenv

  for py_version in "${PYTHON_VERSIONS_TO_VALIDATE[@]}"
  do
    rm -rf ./beam_env_${py_version}
    echo "--------------Setting up virtualenv with $py_version interpreter----------------"
    virtualenv beam_env_${py_version} -p $py_version
    . beam_env_${py_version}/bin/activate

    echo "--------------------------Installing Python SDK-------------------------------"
    pip install apache-beam-${RELEASE_VER}.zip[gcp]

    SHARED_PUBSUB_TOPIC=leader_board-${USER}-python-topic-$(date +%m%d)_$RANDOM
    gcloud pubsub topics create --project=${USER_GCP_PROJECT} ${SHARED_PUBSUB_TOPIC}

    echo "-----------------------Setting up Shell Env Vars------------------------------"
    # [BEAM-4518]
    FIXED_WINDOW_DURATION=20
    cp ~/.bashrc ~/.bashrc_backup
    echo "export USER_GCP_PROJECT=${USER_GCP_PROJECT}" >> ~/.bashrc
    echo "export USER_GCS_BUCKET=${USER_GCS_BUCKET}" >> ~/.bashrc
    echo "export SHARED_PUBSUB_TOPIC=${SHARED_PUBSUB_TOPIC}" >> ~/.bashrc
    echo "export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> ~/.bashrc
    echo "export RELEASE=${RELEASE_VER}" >> ~/.bashrc
    echo "export FIXED_WINDOW_DURATION=${FIXED_WINDOW_DURATION}" >> ~/.bashrc
    echo "export LOCAL_BEAM_DIR=${LOCAL_BEAM_DIR}" >> ~/.bashrc

    echo "--------------------------Updating ~/.m2/settings.xml-------------------------"
    cd ~
    if [[ -d .m2 ]]; then
      mkdir .m2
    fi
    cd .m2
    if [[ -f ~/.m2/settings.xml ]]; then
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
    echo "          <id>Release ${RELEASE_VER} RC${RC_NUM}</id>" >> settings.xml
    echo "          <name>Release ${RELEASE_VER} RC${RC_NUM}</name>" >> settings.xml
    echo "          <url>${REPO_URL}</url>" >> settings.xml
    echo "        </repository>" >> settings.xml
    echo "      </repositories>" >> settings.xml
    echo "    </profile>" >> settings.xml
    echo "  </profiles>" >> settings.xml
    echo "</settings>" >> settings.xml

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

    cd word-count-beam
    echo "A new terminal will pop up and start a java top injector."
    gnome-terminal -x sh -c \
    "echo '******************************************************';
     echo '* Running Pubsub Java Injector';
     echo '******************************************************';
    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
    -Dexec.args='${USER_GCP_PROJECT} ${SHARED_PUBSUB_TOPIC} none';
    exec bash"

    cd ${LOCAL_BEAM_DIR}

    echo "----------------Starting Leaderboard with DirectRunner-----------------------"
    if [[ "$python_leaderboard_direct" = true ]]; then
      LEADERBOARD_DIRECT_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project=${USER_GCP_PROJECT} ${LEADERBOARD_DIRECT_DATASET}
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
      echo "* Skip Python Leaderboard with DirectRunner"
    fi

    echo "----------------Starting Leaderboard with DataflowRunner---------------------"
    if [[ "$python_leaderboard_dataflow" = true ]]; then
      LEADERBOARD_DF_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project=${USER_GCP_PROJECT} ${LEADERBOARD_DF_DATASET}
      echo "export LEADERBOARD_DF_DATASET=${LEADERBOARD_DF_DATASET}" >> ~/.bashrc

      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python Leaderboard with DataflowRunner';
       echo '*****************************************************';
      . ${LOCAL_BEAM_DIR}/beam_env_${py_version}/bin/activate
      python -m apache_beam.examples.complete.game.leader_board \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${SHARED_PUBSUB_TOPIC} \
      --dataset ${LEADERBOARD_DF_DATASET} \
      --runner DataflowRunner \
      --temp_location=${MOBILE_GAME_GCS_BUCKET}/temp/ \
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
      echo "* Skip Python Leaderboard with DataflowRunner"
    fi

    echo "------------------Starting GameStats with DirectRunner-----------------------"
    if [[ "$python_gamestats_direct" = true ]]; then
      GAMESTATS_DIRECT_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project=${USER_GCP_PROJECT} ${GAMESTATS_DIRECT_DATASET}
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
      echo "* Skip Python GameStats with DirectRunner"
    fi

    echo "-------------------Starting GameStats with DataflowRunner--------------------"
    if [[ "$python_gamestats_dataflow" = true ]]; then
      GAMESTATS_DF_DATASET=${USER}_python_validations_$(date +%m%d)_$RANDOM
      bq mk --project=${USER_GCP_PROJECT} ${GAMESTATS_DF_DATASET}
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
      echo "* Skip Python GameStats with DataflowRunner"
    fi
  done # Loop over Python versions.
else
  echo "* Skip Python Leaderboard & GameStates Validations"
fi
