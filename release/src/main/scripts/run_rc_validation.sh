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
  echo "-----------------Stopping multi-language quickstart services-----------------"
  echo "Please stop the Java expansion service manually."
  echo "Please stop the Python expansion service manually."
  echo "Please stop the Python portable runner Job server manually."
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

  if [[ -n `which gcloud` ]]; then
    if [[ -n ${KAFKA_CLUSTER_NAME} ]]; then
        echo "-----------------------Clean up Kafka Cluster on GKE------------------------"
        gcloud container clusters delete --project=${USER_GCP_PROJECT} --region=${USER_GCP_REGION} --async -q ${KAFKA_CLUSTER_NAME}
    fi

    if [[ -n ${SQL_TAXI_TOPIC} ]]; then
        echo "-----------------------Clean up pubsub topic on GCP------------------------"
        gcloud pubsub topics delete --project=${USER_GCP_PROJECT} ${SQL_TAXI_TOPIC}
    fi
  fi
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
declare -a PYTHON_VERSIONS_TO_VALIDATE=("python3.8")
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
echo "TODO(https://github.com/apache/beam/issues/21237): parts of this script launch background processes with gnome-terminal,"
echo "It may not work well over ssh or within a tmux session. Using 'ssh -Y' may help."
echo "[Confirmation Required] Would you like to proceed with current settings? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right configurations."
  exit
fi

echo "[Confirmation Required] Would you like to check published Java artifacts (if you've completed this step for this RC previously, you can safely skip this)? [y|N]"
read confirmation
if [[ $confirmation == "y" ]]; then
  echo "----------------- Checking published Java artifacts (should take ~1 minute) -----------------"

  java_bom=$(curl "${REPO_URL}/org/apache/beam/beam-sdks-java-bom/${RELEASE_VER}/beam-sdks-java-bom-${RELEASE_VER}.pom")
  artifacts=( $(echo $java_bom | grep -Eo "<artifactId>\S+?</artifactId>" | grep -Eo "beam-[a-zA-Z0-9.-]+") )
  if [ ${#artifacts[@]} == 0 ];
  then
      echo "Couldn't find beam-sdks-java-bom in the generated java artifact."
      echo "Please check ${REPO_URL} and try regenerating the java artifacts in the build_rc step."
      exit 1
  fi

  FAILED=()
  for i in "${artifacts[@]}"
  do
      curl "${REPO_URL}/org/apache/beam/${i}/${RELEASE_VER}" -f || FAILED+=($i)
      sleep 0.5
  done
  if [ ${#FAILED[@]} != 0 ];
  then
      echo "Failed to find the following artifacts in the generated java artifact, but they were present as dependencies in beam-sdks-java-bom:"
      for i in "${FAILED[@]}"
      do
          echo "Artifact: ${i} - url: ${REPO_URL}/org/apache/beam/${i}/${RELEASE_VER}"
      done
      echo "Please check ${REPO_URL} and try regenerating the java artifacts in the build_rc step."
      exit 1
  fi
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

if [[ ("$python_xlang_quickstart" = true) \
      || ("$java_xlang_quickstart" = true) ]]; then
  echo "[Confirmation Required] Multi-language quickstart tests require generating
  final Docker tags for several candidate Docker images. This should be safe to do
  so since these tags will be overridden during the Docker image finalization step
  of the Beam release.
  Continue with the release validation ? [y|N]"
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Cannot continue with the validation. Exiting."
    exit
  fi
fi

echo ""
echo "====================Starting Python Quickstart and MobileGame==================="
echo "This task will create a PR against apache/beam, trigger a jenkins job to run:"
echo "1. Python quickstart validations(batch & streaming)"
echo "2. Python MobileGame validations(UserScore, HourlyTeamScore)"
if [[ "$python_quickstart_mobile_game" = true && ! -z `which hub` ]]; then
  touch empty_file.json
  git add empty_file.json
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

# TODO(https://github.com/apache/beam/issues/21193) Run the remaining tests on Jenkins.
echo ""
echo "====================Starting Python Leaderboard & GameStates Validations==============="
if [[ ("$python_leaderboard_direct" = true \
      || "$python_leaderboard_dataflow" = true \
      || "$python_gamestats_direct" = true \
      || "$python_gamestats_dataflow" = true) \
      && ! -z `which gnome-terminal` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz.sha512
  if [[ ! -f apache_beam-${RELEASE_VER}.tar.gz ]]; then
    { echo "Fail to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache_beam-${RELEASE_VER}.tar.gz.sha512

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
    pip install apache_beam-${RELEASE_VER}.tar.gz[gcp]

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
      --sdk_location apache_beam-${RELEASE_VER}.tar.gz; \
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
      --sdk_location apache_beam-${RELEASE_VER}.tar.gz \
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

# Setting up Docker images for multi-language quickstart tests.
if [[ ("$python_xlang_quickstart" = true) \
      || ("$java_xlang_quickstart" = true) ]]; then
  echo ""
  echo "====================Generating Docker tags for multi-language quickstart tests==============="

  RC_DOCKER_TAG=${RELEASE_VER}rc${RC_NUM}
  FINAL_DOCKER_TAG=${RELEASE_VER}

  for py_version in "${PYTHON_VERSIONS_TO_VALIDATE[@]}"
  do
    PYTHON_DOCKER_IMAGE_REPO=apache/beam_${py_version}_sdk
    PYTHON_RC_DOCKER_IMAGE=${PYTHON_DOCKER_IMAGE_REPO}:${RC_DOCKER_TAG}
    PYTHON_FINAL_DOCKER_IMAGE=${PYTHON_DOCKER_IMAGE_REPO}:${FINAL_DOCKER_TAG}
    docker pull ${PYTHON_DOCKER_IMAGE_REPO}:${RC_DOCKER_TAG}
    echo "Creating Docker tag ${FINAL_DOCKER_TAG} from image ${PYTHON_RC_DOCKER_IMAGE}"
    docker tag ${PYTHON_RC_DOCKER_IMAGE} ${PYTHON_FINAL_DOCKER_IMAGE}
  done

  JAVA_DOCKER_IMAGE_REPO=apache/beam_java11_sdk  # Using the default Java version.
  JAVA_RC_DOCKER_IMAGE=${JAVA_DOCKER_IMAGE_REPO}:${RC_DOCKER_TAG}
  JAVA_FINAL_DOCKER_IMAGE=${JAVA_DOCKER_IMAGE_REPO}:${FINAL_DOCKER_TAG}
  docker pull ${JAVA_DOCKER_IMAGE_REPO}:${RC_DOCKER_TAG}
  echo "Creating Docker tag ${FINAL_DOCKER_TAG} from image ${JAVA_RC_DOCKER_IMAGE}"
  docker tag ${JAVA_RC_DOCKER_IMAGE} ${JAVA_FINAL_DOCKER_IMAGE}
fi

echo ""
echo "====================Starting Python Multi-language Quickstart Validations==============="
if [[ ("$python_xlang_quickstart" = true) \
      && ! -z `which gnome-terminal` && ! -z `which kubectl` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz.sha512
  if [[ ! -f apache_beam-${RELEASE_VER}.tar.gz ]]; then
    { echo "Failed to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache_beam-${RELEASE_VER}.tar.gz.sha512

  `which pip` install --upgrade pip
  `which pip` install --upgrade setuptools

  echo "-----------------------Setting up Shell Env Vars------------------------------"
  set_bashrc

  # Run Python Multi-language pipelines under multiple versions of Python using DirectRunner
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
    pip install apache_beam-${RELEASE_VER}.tar.gz

    echo '************************************************************';
    echo '* Running Python Multi-language Quickstart with DirectRunner';
    echo '************************************************************';

    PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX=python_multilang_quickstart
    PYTHON_MULTILANG_QUICKSTART_INPUT_FILE_NAME=${PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX}_input
    PYTHON_MULTILANG_QUICKSTART_OUTPUT_FILE_NAME=${PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX}_output
    PYTHON_MULTILANG_QUICKSTART_EXPECTED_OUTPUT_FILE_NAME=${PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX}_expected_output
    PYTHON_MULTILANG_QUICKSTART_SORTED_OUTPUT_FILE_NAME=${PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX}_sorted_output
    
    # Cleaning up data from any previous runs.
    rm ${PYTHON_MULTILANG_QUICKSTART_FILE_PREFIX}*
    rm ./beam-examples-multi-language-${RELEASE_VER}.jar

    # Generating an input file.
    input_data=( aaa bbb ccc ddd eee)

    touch $PYTHON_MULTILANG_QUICKSTART_INPUT_FILE_NAME
    touch $PYTHON_MULTILANG_QUICKSTART_EXPECTED_OUTPUT_FILE_NAME

    for item in ${input_data[*]}
      do
        echo $item >> $PYTHON_MULTILANG_QUICKSTART_INPUT_FILE_NAME
        echo python:java:$item >> $PYTHON_MULTILANG_QUICKSTART_EXPECTED_OUTPUT_FILE_NAME
      done

    # Downloading the expansion service jar.
    wget ${REPO_URL}/org/apache/beam/beam-examples-multi-language/${RELEASE_VER}/beam-examples-multi-language-${RELEASE_VER}.jar
    JAVA_EXPANSION_SERVICE_PORT=33333
    
    # Starting up the expansion service in a seperate shell.
    echo "A new terminal will pop up and start a java expansion service."
    gnome-terminal -x sh -c \
    "echo '******************************************************';
     echo '* Running Java expansion service in port ${JAVA_EXPANSION_SERVICE_PORT}';
     echo '******************************************************';
     java -jar ./beam-examples-multi-language-${RELEASE_VER}.jar ${JAVA_EXPANSION_SERVICE_PORT};
    exec bash"

    echo "Sleeping 10 seconds for the expansion service to start up."
    sleep 10s

    # Running the pipeline
    python ${LOCAL_BEAM_DIR}/examples/multi-language/python/addprefix.py \
        --runner DirectRunner \
        --environment_type=DOCKER \
        --input $PYTHON_MULTILANG_QUICKSTART_INPUT_FILE_NAME \
        --output $PYTHON_MULTILANG_QUICKSTART_OUTPUT_FILE_NAME \
        --expansion_service_port $JAVA_EXPANSION_SERVICE_PORT

    # Validating output
    cat ${PYTHON_MULTILANG_QUICKSTART_OUTPUT_FILE_NAME}* | sort >> ${PYTHON_MULTILANG_QUICKSTART_SORTED_OUTPUT_FILE_NAME}

    if cmp --silent -- $PYTHON_MULTILANG_QUICKSTART_EXPECTED_OUTPUT_FILE_NAME $PYTHON_MULTILANG_QUICKSTART_SORTED_OUTPUT_FILE_NAME; then
      echo "Successfully validated Python multi-language quickstart example. No additional manual validation needed."
    else
      echo "Python multi-language quickstart output validation failed. Since the output of the pipeline did not match the expected output"
      echo "Expected output:\n"
      cat $PYTHON_MULTILANG_QUICKSTART_EXPECTED_OUTPUT_FILE_NAME
      echo "\n"
      echo "Pipeline output:\n"
      cat $PYTHON_MULTILANG_QUICKSTART_SORTED_OUTPUT_FILE_NAME
      echo "\n"
      exit 1
    fi
  done # Loop over Python versions.
else
  echo "* Skipping Python Multi-language Quickstart Validations"
fi

echo ""
echo "====================Starting Java Multi-language Quickstart Validations==============="
if [[ ("$java_xlang_quickstart" = true) \
      && ! -z `which gnome-terminal` && ! -z `which kubectl` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz.sha512
  if [[ ! -f apache_beam-${RELEASE_VER}.tar.gz ]]; then
    { echo "Failed to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache_beam-${RELEASE_VER}.tar.gz.sha512

  `which pip` install --upgrade pip
  `which pip` install --upgrade setuptools

  echo "-----------------------Setting up Shell Env Vars------------------------------"
  set_bashrc

  # Run Java Multi-language pipelines under multiple versions of Python using DirectRunner
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
    pip install apache_beam-${RELEASE_VER}.tar.gz[dataframe]

    # Deacrivating in the main shell. We will reactivate the virtual environment new shells
    # for the expansion service and the job server.
    deactivate

    PYTHON_PORTABLE_RUNNER_JOB_SERVER_PORT=44443
    PYTHON_EXPANSION_SERVICE_PORT=44444

    # Starting up the Job Server
    echo "A new terminal will pop up and start a Python PortableRunner job server."
    gnome-terminal -x sh -c \
    "echo '******************************************************';
     echo '* Running Python PortableRunner in port ${PYTHON_PORTABLE_RUNNER_JOB_SERVER_PORT}';
     echo '******************************************************';
     . ./beam_env_${py_version}/bin/activate;
     python -m apache_beam.runners.portability.local_job_service_main -p ${PYTHON_PORTABLE_RUNNER_JOB_SERVER_PORT};
    exec bash"

    # Starting up the Python expansion service
    echo "A new terminal will pop up and start a Python expansion service."
    gnome-terminal -x sh -c \
    "echo '******************************************************';
     echo '* Running Python Portabexpansion service in port ${PYTHON_EXPANSION_SERVICE_PORT}';
     echo '******************************************************';
     . ./beam_env_${py_version}/bin/activate;
     python -m apache_beam.runners.portability.expansion_service_main --port=${PYTHON_EXPANSION_SERVICE_PORT} \
         --fully_qualified_name_glob=* \
         --pickle_library=cloudpickle;
    exec bash"

    echo "Sleeping 10 seconds for the job server and the expansion service to start up."
    sleep 10s

    echo '************************************************************';
    echo '* Running Java Multi-language Quickstart with DirectRunner';
    echo '************************************************************';

    JAVA_MULTILANG_QUICKSTART_FILE_PREFIX=java_multilang_quickstart
    JAVA_MULTILANG_QUICKSTART_OUTPUT_FILE_NAME=${JAVA_MULTILANG_QUICKSTART_FILE_PREFIX}_output

    ./gradlew :examples:multi-language:pythonDataframeWordCount -Pver=${RELEASE_VER} -Prepourl=${REPO_URL} --args=" \
    --runner=PortableRunner \
    --jobEndpoint=localhost:${PYTHON_PORTABLE_RUNNER_JOB_SERVER_PORT} \
    --expansionService=localhost:${PYTHON_EXPANSION_SERVICE_PORT} \
    --output=${JAVA_MULTILANG_QUICKSTART_OUTPUT_FILE_NAME}"

    # We cannot validate local output since 
    # TODO: Write output to GCS and validate when Python portable runner can forward credentials to GCS appropriately.

    java_xlang_quickstart_status=$?
    if [[ $java_xlang_quickstart_status -eq 0 ]]; then
      echo "Successfully completed Java multi-language quickstart example. No manual validation needed."
    else
      { echo "Java multi-language quickstart failed since the pipeline execution failed." ;exit 1; }
    fi
  done # Loop over Python versions.
else
  echo "* Skipping Java Multi-language Quickstart Validations"
fi

echo ""
echo "====================Starting Python Multi-language Validations with DataflowRunner==============="
if [[ ("$python_xlang_kafka_taxi_dataflow" = true
      || "$python_xlang_sql_taxi_dataflow" = true) \
      && ! -z `which gnome-terminal` && ! -z `which kubectl` ]]; then
  cd ${LOCAL_BEAM_DIR}

  echo "---------------------Downloading Python Staging RC----------------------------"
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz
  wget ${PYTHON_RC_DOWNLOAD_URL}/${RELEASE_VER}/python/apache_beam-${RELEASE_VER}.tar.gz.sha512
  if [[ ! -f apache_beam-${RELEASE_VER}.tar.gz ]]; then
    { echo "Fail to download Python Staging RC files." ;exit 1; }
  fi

  echo "--------------------------Verifying Hashes------------------------------------"
  sha512sum -c apache_beam-${RELEASE_VER}.tar.gz.sha512

  `which pip` install --upgrade pip
  `which pip` install --upgrade setuptools

  echo "-----------------------Setting up Shell Env Vars------------------------------"
  set_bashrc

  echo "-----------------------Setting up Kafka Cluster on GKE------------------------"
  KAFKA_CLUSTER_NAME=xlang-kafka-cluster-$RANDOM
  if [[ "$python_xlang_kafka_taxi_dataflow" = true ]]; then
    gcloud container clusters create --project=${USER_GCP_PROJECT} --region=${USER_GCP_REGION} --no-enable-ip-alias $KAFKA_CLUSTER_NAME
    kubectl apply -R -f ${LOCAL_BEAM_DIR}/.test-infra/kubernetes/kafka-cluster
    echo "* Please wait for 10 mins to let a Kafka cluster be launched on GKE."
    echo "* Sleeping for 10 mins"
    sleep 10m
  else
    echo "* Skipping Kafka cluster setup"
  fi

  # Run Python multi-language pipelines under multiple versions of Python using Dataflow Runner
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
    pip install apache_beam-${RELEASE_VER}.tar.gz[gcp]

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
      --sdk_location apache_beam-${RELEASE_VER}.tar.gz; \
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
      --sdk_location apache_beam-${RELEASE_VER}.tar.gz; \
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
  echo "* Skipping Python Multi-language Dataflow Validations"
fi
echo "*************************************************************"
echo " NOTE: Streaming pipelines are not automatically canceled.   "
echo " Please manually cancel any remaining test pipelines and     "
echo " clean up the resources (BigQuery dataset, PubSub topics and "
echo " subscriptions, GKE cluster, etc.) after verification.       "
echo "*************************************************************"
