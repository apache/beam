#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script installs Apache Flink (http://flink.apache.org) on a Google Cloud
# Dataproc cluster. This script is based on previous scripts:
# https://github.com/GoogleCloudPlatform/bdutil/tree/master/extensions/flink
#
# To use this script, you will need to configure the following variables to
# match your cluster. For information about which software components
# (and their version) are included in Cloud Dataproc clusters, see the
# Cloud Dataproc Image Version information:
# https://cloud.google.com/dataproc/concepts/dataproc-versions
#
# This file originated from:
# https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/flink/flink.sh
# (last commit: 6477e6067cc7a08de165117778a251ac2ed6a62f)
set -euxo pipefail

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

# Install directories for Flink and Hadoop.
readonly FLINK_INSTALL_DIR='/usr/lib/flink'
readonly FLINK_WORKING_DIR='/var/lib/flink'
readonly FLINK_YARN_SCRIPT='/usr/bin/flink-yarn-daemon'
readonly FLINK_WORKING_USER='yarn'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'

# Heap memory used by the job manager (master) determined by the physical (free) memory of the server.
# Flink config entry: jobmanager.heap.mb.
readonly FLINK_JOBMANAGER_MEMORY_FRACTION='1.0'

# Heap memory used by the task managers (slaves) determined by the physical (free) memory of the servers.
# Flink config entry: taskmanager.memory.process.size.
readonly FLINK_TASKMANAGER_MEMORY_FRACTION='1.0'

readonly START_FLINK_YARN_SESSION_METADATA_KEY='flink-start-yarn-session'
# Set this to true to start a flink yarn session at initialization time.
readonly START_FLINK_YARN_SESSION_DEFAULT=true

# Set this to install flink from a snapshot URL instead of apt
readonly FLINK_SNAPSHOT_URL_METADATA_KEY='flink-snapshot-url'

# Set this to install pre-packaged Hadoop jar
readonly HADOOP_JAR_URL_METADATA_KEY='hadoop-jar-url'

# Set this to define how many task slots are there per flink task manager
readonly FLINK_TASKMANAGER_SLOTS_METADATA_KEY='flink-taskmanager-slots'


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function retry_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_command "apt-get install -y $pkgs"
}

function install_flink_snapshot() {
  local work_dir="$(mktemp -d)"
  local flink_url="$(/usr/share/google/get_metadata_value "attributes/${FLINK_SNAPSHOT_URL_METADATA_KEY}")"
  local hadoop_url="$(/usr/share/google/get_metadata_value "attributes/${HADOOP_JAR_URL_METADATA_KEY}")"
  local flink_local="${work_dir}/flink.tgz"
  local flink_toplevel_pattern="${work_dir}/flink-*"

  pushd "${work_dir}"

  curl -o "${flink_local}" "${flink_url}"
  tar -xzvf "${flink_local}"
  rm "${flink_local}"
  # only the first match of the flink toplevel pattern is used
  local files=( ${flink_toplevel_pattern} )
  local flink_toplevel="${files[0]}"
  mv "${flink_toplevel}" "${FLINK_INSTALL_DIR}"

  popd # work_dir

  if [[ ! -z "${hadoop_url}" ]]; then
    cd "${FLINK_INSTALL_DIR}/lib"; curl -O "${hadoop_url}"
  fi
}

function configure_flink() {
  # Number of worker nodes in your cluster
  local num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)

  # Number of Flink TaskManagers to use. Reserve 1 node for the JobManager.
  # NB: This assumes > 1 worker node.
  local num_taskmanagers="$(($num_workers - 1))"

  local num_cores="$(grep -c processor /proc/cpuinfo)"
  local flink_taskmanager_slots_default=$num_cores

  local slots="$(/usr/share/google/get_metadata_value \
    "attributes/${START_FLINK_YARN_SESSION_METADATA_KEY}" \
    || echo "${START_FLINK_YARN_SESSION_DEFAULT}")"

  # if provided, use user defined number of slots.
  local flink_taskmanager_slots="$(/usr/share/google/get_metadata_value \
    "attributes/${FLINK_TASKMANAGER_SLOTS_METADATA_KEY}" \
    || echo "${flink_taskmanager_slots_default}")"

  # Determine the default parallelism.
  local flink_parallelism=$(python -c \
    "print(${num_taskmanagers} * ${flink_taskmanager_slots})")

  # Get worker memory from yarn config.
  local worker_total_mem="$(hdfs getconf \
    -confKey yarn.nodemanager.resource.memory-mb)"
  local flink_jobmanager_memory=$(python -c \
    "print(int(${worker_total_mem} * ${FLINK_JOBMANAGER_MEMORY_FRACTION}))")
  local flink_taskmanager_memory=$(python -c \
    "print(int(${worker_total_mem} * ${FLINK_TASKMANAGER_MEMORY_FRACTION}))")

  # Fetch the primary master name from metadata.
  local master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

  # Use the staging bucket to store checkpoints
  local checkpoints_dir="gs://$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)/checkpoints"

  # create working directory
  mkdir -p "${FLINK_WORKING_DIR}"

  # Apply Flink settings by appending them to the default config.
  cat << EOF >> ${FLINK_INSTALL_DIR}/conf/flink-conf.yaml
# Settings applied by Cloud Dataproc initialization action
jobmanager.rpc.address: ${master_hostname}
jobmanager.heap.mb: ${flink_jobmanager_memory}
taskmanager.memory.process.size: "${flink_taskmanager_memory} mb"
taskmanager.memory.jvm-metaspace.size: 512 mb
taskmanager.memory.task.off-heap.size: 256 mb
taskmanager.memory.managed.fraction: 0.5
taskmanager.numberOfTaskSlots: ${flink_taskmanager_slots}
parallelism.default: ${flink_parallelism}
fs.hdfs.hadoopconf: ${HADOOP_CONF_DIR}
state.backend: filesystem
state.checkpoints.dir: ${checkpoints_dir}
EOF

cat > "${FLINK_YARN_SCRIPT}" << EOF
#!/bin/bash
set -exuo pipefail
sudo -u yarn -i \
HADOOP_CONF_DIR=${HADOOP_CONF_DIR} \
  ${FLINK_INSTALL_DIR}/bin/yarn-session.sh \
  -s "${flink_taskmanager_slots}" \
  -jm "${flink_jobmanager_memory}" \
  -tm "${flink_taskmanager_memory}" \
  -nm flink-dataproc \
  --detached
EOF
chmod +x "${FLINK_YARN_SCRIPT}"

}

function start_flink_master() {
  local start_yarn_session="$(/usr/share/google/get_metadata_value \
    "attributes/${START_FLINK_YARN_SESSION_METADATA_KEY}" \
    || echo "${START_FLINK_YARN_SESSION_DEFAULT}")"

  if ${start_yarn_session} ; then
    "${FLINK_YARN_SCRIPT}"
  else
    echo "Doing nothing"
  fi
}

function main() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  local snapshot_url="$(/usr/share/google/get_metadata_value \
    "attributes/${FLINK_INSTALL_SNAPSHOT_METADATA_KEY}" \
    || echo "${FLINK_INSTALL_SNAPSHOT_METADATA_DEFAULT}")"

  # check if a flink snapshot URL is specified
  if /usr/share/google/get_metadata_value \
    "attributes/${FLINK_SNAPSHOT_URL_METADATA_KEY}" ; then
      install_flink_snapshot || err "Unable to install Flink"
  else
    update_apt_get || err "Unable to update apt-get"
    install_apt_get flink || err "Unable to install flink"
  fi

  configure_flink || err "Flink configuration failed"
  if [[ "${role}" == 'Master' ]] ; then
    (retry_command start_flink_master) || err "Unable to start Flink master"
  fi
}

main
