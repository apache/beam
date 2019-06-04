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

function clean_up(){
  echo "======================Stopping Pubsub Java Injector========================="
  echo "Please stop java injector manually."
  echo "===========================Signing up Spreadsheet==========================="
  echo "Please open this spreadsheet: https://s.apache.org/beam-release-validation"
  echo "Please sign up your name in the tests you have ran."

  echo "===========================Final Cleanup==========================="
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

  rm -rf ~/{LOCAL_CLONE_DIR}
  echo "* Deleted working dir ~/{LOCAL_CLONE_DIR}"
}

RELEASE=
REPO_URL=
RC_NUM=
RELEASE_BRANCH=
WORKING_BRANCH=
LOCAL_CLONE_DIR=rc_validations
BEAM_ROOT_DIR=beam
GIT_REPO_URL=https://github.com/apache/beam.git
PYTHON_RC_DOWNLOAD_URL=https://dist.apache.org/repos/dist/dev/beam
HUB_VERSION=2.5.0
HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
declare -a DEFAULT_PYTHON_VERSIONS_TO_VALIDATE=("python2.7" "python3.5")

echo "[Input Required] Please enter the release version: "
read RELEASE
RELEASE_BRANCH=release-${RELEASE}
WORKING_BRANCH=release-${RELEASE}-RC${RC_NUM}_validations
echo "[Input Required] Please enter the release candidate number(e.g. 1): "
read RC_NUM
echo "[Input Required] Please copy the repo URL from the vote email sent out by Release Manager:"
echo "The URL should look like: https://repository.apache.org/content/repositories/orgapachebeam-0000"
read REPO_URL

echo "====================Checking Environment Variables================="
echo "running validations on release ${RELEASE} RC${RC_NUM}."
echo "repo URL for this RC: ${REPO_URL}"
echo "[Confirmation Required] Do you confirm all information above are correct? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "=================Checking hub========================"
if [[ -z `which hub` ]]; then
  echo "There is no hub installed on your machine."
  echo "Would you like to install hub with root permission? [y|N]"
  read confirmation
  if [[ $confirmation != "y"  ]]; then
    echo "Refused to install hub. Cannot proceed into next setp."
    exit
  fi
  echo "=================Installing hub======================="
  wget https://github.com/github/hub/releases/download/v${HUB_VERSION}/${HUB_ARTIFACTS_NAME}.tgz
  tar zvxvf ${HUB_ARTIFACTS_NAME}.tgz
  sudo ./${HUB_ARTIFACTS_NAME}/install
  echo "eval "$(hub alias -s)"" >> ~/.bashrc
  rm -rf ${HUB_ARTIFACTS_NAME}*
fi
hub version


echo "====================Cloning Beam Release Branch===================="
cd ~
if [[ -d ${LOCAL_CLONE_DIR} ]]; then
  rm -rf ${LOCAL_CLONE_DIR}
fi

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
  echo "*************************************************************"
  echo "* Running Java Quickstart with DirectRunner"
  echo "*************************************************************"
  ./gradlew :runners:direct-java:runQuickstartJavaDirect \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Apex local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Apex local runner"
  echo "*************************************************************"
  ./gradlew :runners:apex:runQuickstartJavaApex \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Flink local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Flink local runner"
  echo "*************************************************************"
  ./gradlew :runners:flink:1.5:runQuickstartJavaFlinkLocal \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Spark local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "*************************************************************"
  echo "* Running Java Quickstart with Spark local runner"
  echo "*************************************************************"
  ./gradlew :runners:spark:runQuickstartJavaSpark \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "====================Checking Google Cloud SDK======================"
if [[ -z `which gcloud` ]]; then
  echo "You don't have Google Cloud SDK installed."
  echo " Do you want to install gcloud with sudo permission? [y|N]"
  read confirmation
  if [[ $confirmation != 'y' ]]; then
    echo "Exit script without running rest validations."
    exit
  fi
  sudo apt-get install google-cloud-sdk
fi
gcloud --version

echo "[Current task] Java quickstart with Dataflow runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[GCP Project Required] Please input your GCP project:"
  read USER_GCP_PROJECT
  echo "[GCP GCS Bucket Required] Please input your GCS bucket: "
  read USER_GCS_BUCKET
  echo "[gcloud Login Required] Please login into your gcp account: "
  gcloud auth application-default login
  GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json

  echo "*************************************************************"
  echo "* Running Java Quickstart with DataflowRunner"
  echo "*************************************************************"
  gcloud auth application-default login
  gcloud config set project ${USER_GCP_PROJECT}
  ./gradlew :runners:google-cloud-dataflow-java:runQuickstartJavaDataflow \
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
  echo "[GCP GCS Bucket Required] Please input your GCS bucket: "
  read USER_GCS_BUCKET
  MOBILE_GAME_DATASET=${USER}_java_validations
  MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-java-topic-1
  echo "Please review following GCP sources setup: "
  echo "Using GCP project: ${USER_GCP_PROJECT}"
  echo "Will create BigQuery dataset: ${MOBILE_GAME_DATASET}"
  echo "Will create Pubsub topic: ${MOBILE_GAME_PUBSUB_TOPIC}"
  echo "[Confirmation Required] Do you want to run validations with configurations above? [y|N]"
  read confirmation
  if [[ $confirmation = "y" ]]; then
    gcloud auth login
    gcloud config set project ${USER_GCP_PROJECT}
    echo "-----------------Setting Up Service Account------------------------"
    echo "Please go to GCP IAM console under your project(${USER_GCP_PROJECT})."
    echo "Create a service account as project owner, if you don't have one."
    echo "[Input Required] Please enter your service account email:"
    read USER_SERVICE_ACCOUNT_EMAIL
    SERVICE_ACCOUNT_KEY_JSON=${USER}_json_key.json
    gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_EMAIL}
    export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/${SERVICE_ACCOUNT_KEY_JSON}

    echo "-------------------Creating BigQuery Dataset-----------------------"
    bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
    bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

    echo "----------------------Creating Pubsub Topic------------------------"
    gcloud alpha pubsub topics delete projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC}
    gcloud alpha pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

    echo "**************************************************************************"
    echo "* Java mobile game validations: UserScore, HourlyTeamScore, Leaderboard"
    echo "**************************************************************************"
    ./gradlew :runners:google-cloud-dataflow-java:runMobileGamingJavaDataflow \
    -Prepourl=${REPO_URL} \
    -Pver=${RELEASE} \
    -PgcpProject=${USER_GCP_PROJECT} \
    -PgcsBucket=${USER_GCS_BUCKET} \
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
  echo "[Input Required] Please enter your github username:"
  read GITHUB_USERNAME
  WORKING_BRANCH=python_validation_pr
  git checkout -b ${WORKING_BRANCH}
  touch empty_file.txt
  git add empty_file.txt
  git commit -m "Add empty file in order to create PR"
  git push -f ${USER_REMOTE_URL}
  hub pull-request -b apache:${RELEASE_BRANCH} -h ${GITHUB_USERNAME}:${WORKING_BRANCH} -F- <<<"[DO NOT MERGE]Run Python RC Validation Tests


  Run Python ReleaseCandidate"

  echo "[NOTE] If there is no jenkins job started, please comment generated PR with: Run Python ReleaseCandidate"
fi

echo "==============Starting Python Leaderboard & GameStates Validations==============="
echo "This task asks for GCP resources. Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  cd ~/${LOCAL_CLONE_DIR}

  echo "---------------------Checking gnome-terminal----------------------------------"
  if [[ -z `which gnome-terminal` ]]; then
    echo "You don't have gnome-terminal installed."
    echo "Do you want to install gnome-terminal with sudo permission? [y|N]"
    read confirmation
    if [[ $confirmation != 'y' ]]; then
      echo "Exit this script without proceeding to the next step."
      exit
    fi
  fi
  gnome-terminal --version

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE}/python/apache-beam-${RELEASE}.zip
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE}/python/apache-beam-${RELEASE}.zip.sha512

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache-beam-${RELEASE}.zip.sha512

  sudo `which pip` install --upgrade pip
  sudo `which pip` install --upgrade setuptools
  sudo `which pip` install --upgrade virtualenv

  echo "[Input Required] Please enter Python interpreter(s) separated by space to use for running validation steps."
  echo "Sample input: python2.7"
  echo "Enter empty line to repeat validation steps using all of ${DEFAULT_PYTHON_VERSIONS_TO_VALIDATE[@]}."

  read -a PYTHON_VERSIONS_TO_VALIDATE
  if [[ -z "$PYTHON_VERSIONS_TO_VALIDATE" ]]; then
    PYTHON_VERSIONS_TO_VALIDATE=${DEFAULT_PYTHON_VERSIONS_TO_VALIDATE[@]}
  fi

  for py_version in "${PYTHON_VERSIONS_TO_VALIDATE[@]}"
  do
    rm -rf ./beam_env_${py_version}
    echo "--------------Setting up virtualenv with $py_version interpreter----------------"
    virtualenv beam_env_${py_version} -p $py_version
    . beam_env_${py_version}/bin/activate

    echo "--------------------------Installing Python SDK-------------------------------"
    pip install apache-beam-${RELEASE}.zip
    pip install apache-beam-${RELEASE}.zip[gcp]

    echo "----------------------------Setting up GCP Sources----------------------------"
    echo "[GCP Project Required] Please input your GCP project:"
    read USER_GCP_PROJECT
    gcloud auth login
    gcloud config set project ${USER_GCP_PROJECT}

    MOBILE_GAME_GCS_BUCKET=gs://${USER}_python_validations_bucket
    MOBILE_GAME_DATASET=${USER}_python_validations
    MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-python-topic-1
    gsutil mb -p ${USER_GCP_PROJECT} ${MOBILE_GAME_GCS_BUCKET}
    gcloud alpha pubsub topics delete projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC}
    gcloud alpha pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

    echo "-----------------------Setting Up Service Account-----------------------------"
    echo "Please go to GCP IAM console under your project(${USER_GCP_PROJECT})."
    echo "Create a service account as project owner, if you don't have one."
    echo "[Input Required] Please enter your service account email:"
    read USER_SERVICE_ACCOUNT_EMAIL
    SERVICE_ACCOUNT_KEY_JSON=${USER}_json_key.json
    gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_EMAIL}
    export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/${SERVICE_ACCOUNT_KEY_JSON}

    echo "-----------------------Setting up Shell Env Vars------------------------------"
    # [BEAM-4518]
    FIXED_WINDOW_DURATION=20
    cp ~/.bashrc ~/.bashrc_backup
    echo "export USER_GCP_PROJECT=${USER_GCP_PROJECT}" >> ~/.bashrc
    echo "export MOBILE_GAME_DATASET=${MOBILE_GAME_DATASET}" >> ~/.bashrc
    echo "export MOBILE_GAME_PUBSUB_TOPIC=${MOBILE_GAME_PUBSUB_TOPIC}" >> ~/.bashrc
    echo "export MOBILE_GAME_GCS_BUCKET=${MOBILE_GAME_GCS_BUCKET}" >> ~/.bashrc
    echo "export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" >> ~/.bashrc
    echo "export RELEASE=${RELEASE}" >> ~/.bashrc
    echo "export FIXED_WINDOW_DURATION=${FIXED_WINDOW_DURATION}" >> ~/.bashrc


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
    gnome-terminal -x sh -c \
    "echo '******************************************************';
     echo '* Running Pubsub Java Injector';
     echo '******************************************************';
    mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
    -Dexec.args='${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC} none';
    exec bash"

    echo "[Confirmation Required] Please enter y to confirm injector running:"
    read confirmation
    if [[ $confirmation != "y" ]]; then
      echo "Following tests only can be ran when java injector running."
      clean_up
      exit
    fi

    cd ~/${LOCAL_CLONE_DIR}/

    echo "----------------Starting Leaderboard with DirectRunner-----------------------"
    echo "[Confirmation Required] Do you want to proceed? [y|N]"
    read confirmation
    if [[ $confirmation = "y" ]]; then
      bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python Leaderboard with DirectRunner';
       echo '*****************************************************';
      python -m apache_beam.examples.complete.game.leader_board \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
      --dataset ${MOBILE_GAME_DATASET};
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 5 mins to let results get populated."
      echo "* Sleeping for 5 mins"
      sleep 5m
      echo "***************************************************************"
      echo "* How to verify results:"
      echo "* 1. Check whether there is any error messages in the task running terminal."
      echo "* 2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has leader_board_users and leader_board_teams table."
      echo "* 3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
      bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_users
      echo "* 4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
      bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_teams
      echo "***************************************************************"

      echo "If you have verified all items listed above, please terminate the python job."
      echo "[Confirmation Required] Please confirm whether you have stopped this job: [y|N]"
      read confirmation
      if [[ $confirmation != "y" ]]; then
        echo "Current job must be terminated in order to proceed into next test."
        clean_up
        exit
      fi
    fi

    echo "----------------Starting Leaderboard with DataflowRunner---------------------"
    echo "[Confirmation Required] Do you want to proceed? [y|N]"
    read confirmation
    if [[ $confirmation = "y" ]]; then
      bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      echo "This is a streaming job. This task will be launched in a separate terminal."
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running Python Leaderboard with DataflowRunner';
       echo '*****************************************************';
      python -m apache_beam.examples.complete.game.leader_board \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
      --dataset ${MOBILE_GAME_DATASET} \
      --runner DataflowRunner \
      --temp_location=${MOBILE_GAME_GCS_BUCKET}/temp/ \
      --sdk_location apache-beam-${RELEASE}.zip; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 10 mins to let Dataflow job be launched and results get populated."
      echo "* Sleeping for 10 mins"
      sleep 10m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has leader_board_users and leader_board_teams table."
      echo "* 3. Check whether leader_board_users has data, retrieving BigQuery data as below: "
      bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_users
      echo "* 4. Check whether leader_board_teams has data, retrieving BigQuery data as below:"
      bq head -n 10 ${MOBILE_GAME_DATASET}.leader_board_teams
      echo "***************************************************************"

      echo "If you have verified all items listed above, please terminate this job in Dataflow Console."
      echo "[Confirmation Required] Please confirm whether you have stopped this job: [y|N]"
      read confirmation
      if [[ $confirmation != "y" ]]; then
        echo "Current job must be terminated in order to proceed into next test."
        clean_up
        exit
      fi
    fi

    echo "------------------Starting GameStats with DirectRunner-----------------------"
    echo "[Confirmation Required] Do you want to proceed? [y|N]"
    read confirmation
    if [[ $confirmation = "y" ]]; then
      bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

      echo "This is a streaming job. This task will be launched in a separate terminal."
      echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running GameStats with DirectRunner';
       echo '*****************************************************';
      python -m apache_beam.examples.complete.game.game_stats \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
      --dataset ${MOBILE_GAME_DATASET} \
      --fixed_window_duration ${FIXED_WINDOW_DURATION}; \
      exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 25 mins to let results get populated."
      echo "* Sleeping for 25mins"
      sleep 25m
      echo "* How to verify results:"
      echo "* 1. Check whether there is any error messages in the task running terminal."
      echo "* 2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has game_stats_teams and game_stats_sessions table."
      echo "* 3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
      bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_teams
      echo "* 4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
      bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_sessions
      echo "***************************************************************"

      echo "If you have verified all items listed above, please terminate the python job."
      echo "[Confirmation Required] Please confirm whether you have stopped this job: [y|N]"
      read confirmation
      if [[ $confirmation != "y" ]]; then
        echo "Current job must be terminated in order to proceed into next test."
        clean_up
        exit
      fi
    fi

    echo "-------------------Starting GameStats with DataflowRunner--------------------"
    echo "[Confirmation Required] Do you want to proceed? [y|N]"
    read confirmation
    if [[ $confirmation = "y" ]]; then
      bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
      echo "This is a streaming job. This task will be launched in a separate terminal."
      echo "Streaming job is running with fixed_window_duration=${FIXED_WINDOW_DURATION}"
      gnome-terminal -x sh -c \
      "echo '*****************************************************';
       echo '* Running GameStats with DataflowRunner';
       echo '*****************************************************';
      python -m apache_beam.examples.complete.game.game_stats \
      --project=${USER_GCP_PROJECT} \
      --topic projects/${USER_GCP_PROJECT}/topics/${MOBILE_GAME_PUBSUB_TOPIC} \
      --dataset ${MOBILE_GAME_DATASET} \
      --runner DataflowRunner \
      --temp_location=${MOBILE_GAME_GCS_BUCKET}/temp/ \
      --sdk_location apache-beam-${RELEASE}.zip \
      --fixed_window_duration ${FIXED_WINDOW_DURATION}; exec bash"

      echo "***************************************************************"
      echo "* Please wait for at least 30 mins to let results get populated."
      echo "* Sleeping for 30 mins"
      sleep 30m
      echo "* How to verify results:"
      echo "* 1. Goto your Dataflow job console and check whether there is any error."
      echo "* 2. Goto your BigQuery console and check whether your ${MOBILE_GAME_DATASET} has game_stats_teams and game_stats_sessions table."
      echo "* 3. Check whether game_stats_teams has data, retrieving BigQuery data as below: "
      bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_teams
      echo "* 4. Check whether game_stats_sessions has data, retrieving BigQuery data as below:"
      bq head -n 10 ${MOBILE_GAME_DATASET}.game_stats_sessions
      echo "***************************************************************"

      echo "If you have verified all items listed above, please terminate the python job."
      echo "[Confirmation Required] Please confirm whether you have stopped this job: [y|N]"
      read confirmation
      if [[ $confirmation != "y" ]]; then
        echo "Current job must be terminated in order to proceed into next test."
        clean_up
        exit
      fi
    fi
  done # Loop over Python versions.
fi

clean_up
