#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# A simple launcher for the Apache Beam SQL Shell.
# This script builds a self-contained JAR with all dependencies using Maven,
# which correctly handles service loading for IOs, and caches the JAR.
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
DEFAULT_BEAM_VERSION="2.67.0"
MAIN_CLASS="org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine"
# Directory to store cached executable JAR files
CACHE_DIR="${HOME}/.beamshell/cache"
mkdir -p "${CACHE_DIR}"

# Create a temporary directory for our Maven project.
WORK_DIR=$(mktemp -d)

# --- Helper Functions ---
# This function downloads the maven wrapper script and supporting files.
function setup_maven_wrapper() {
  echo "🔧 Setting up Maven Wrapper..."
  local wrapper_dir="${WORK_DIR}/.mvn/wrapper"
  mkdir -p "${wrapper_dir}"

  # Define URLs for a stable version of the wrapper files
  local mvnw_script_url="https://raw.githubusercontent.com/apache/maven-wrapper/maven-wrapper-3.2.0/src/main/wrapper/mvnw"
  local wrapper_jar_url="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.2.0/maven-wrapper-3.2.0.jar"

  # We will create the properties file ourselves to specify a modern Maven version
  echo "distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip" > "${wrapper_dir}/maven-wrapper.properties"

  # Download the mvnw script and the wrapper JAR
  curl -sSL -o "${WORK_DIR}/mvnw" "${mvnw_script_url}"
  curl -sSL -o "${wrapper_dir}/maven-wrapper.jar" "${wrapper_jar_url}"

  # Make the wrapper script executable
  chmod +x "${WORK_DIR}/mvnw"
}

function usage() {
  echo "Usage: $0 [--version <beam_version>] [--runner <runner_name>] [--io <io_connector>]..."
  echo ""
  echo "A self-contained launcher for the Apache Beam SQL Shell."
  echo ""
  echo "Options:"
  echo "  --version   Specify the Apache Beam version (default: ${DEFAULT_BEAM_VERSION})."
  echo "  --runner    Specify the Beam runner to use (default: direct). Supported: direct, dataflow."
  echo "  --io        Specify an IO connector to include (e.g., iceberg, kafka). Can be used multiple times."
  echo "  --list-versions      List all available Beam versions from Maven Central and exit."
  echo "  -h, --help  Show this help message."
  exit 1
}

# This function fetches all available Beam versions from Maven Central.
function list_versions() {
  echo "🔎 Fetching the 10 most recent Apache Beam versions from Maven Central..."
  local metadata_url="https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/maven-metadata.xml"

  if ! command -v curl &> /dev/null; then
    echo "❌ Error: 'curl' is required to fetch the version list." >&2
    return 1
  fi

  # Fetch, parse, filter, sort, and take the top 10.
  local versions
  versions=$(curl -sS "${metadata_url}" | \
    grep '<version>' | \
    sed 's/.*<version>\(.*\)<\/version>.*/\1/' | \
    grep -v 'SNAPSHOT' | \
    sort -rV | \
    head -n 10) # Limit to the first 10 lines

  if [ -z "${versions}" ]; then
    echo "❌ Could not retrieve versions. Please check your internet connection or the Maven Central status." >&2
    return 1
  fi

  echo "✅ 10 latest versions:"
  echo "${versions}"
}

# This function ensures our temporary directory is cleaned up when the script exits.
function cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT # Register the cleanup function to run on script exit

# --- Argument Parsing ---
BEAM_VERSION="${DEFAULT_BEAM_VERSION}"
IO_CONNECTORS=()
BEAM_RUNNER="direct"
SQLLINE_ARGS=()
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --version) BEAM_VERSION="$2"; shift ;;
    --runner) BEAM_RUNNER=$(echo "$2" | tr '[:upper:]' '[:lower:]'); shift ;;
    --io) IO_CONNECTORS+=("$2"); shift ;;
    --list-versions) list_versions; exit 0 ;;
    -h|--help) usage ;;
    *) SQLLINE_ARGS+=("$1") ;;
  esac
  shift
done

# --- Prerequisite Check ---
# Java is always required.
if ! command -v java &> /dev/null; then
  echo "❌ Error: 'java' command not found. It is required to run the application." >&2
  exit 1
fi

# Decide which Maven command to use. Prefer system 'mvn'.
MAVEN_CMD=""
if command -v mvn &> /dev/null; then
  echo "🔧 Found system Maven. Using 'mvn'."
  MAVEN_CMD="mvn"
else
  echo "🔧 System 'mvn' not found. Setting up Maven Wrapper."
  # Check for curl, which is required for the fallback wrapper setup.
  if ! command -v curl &> /dev/null; then
    echo "❌ Error: 'curl' is required to download the Maven wrapper, as system 'mvn' was not found." >&2
    exit 1
  fi
  setup_maven_wrapper
  MAVEN_CMD="${WORK_DIR}/mvnw"
fi

echo "🚀 Preparing Beam SQL Shell v${BEAM_VERSION}..."
echo "    Runner: ${BEAM_RUNNER}"
if [ ${#IO_CONNECTORS[@]} -gt 0 ]; then
  echo "    Including IOs: ${IO_CONNECTORS[*]}"
fi

# --- Dependency Resolution & JAR Caching ---

# Create a unique key for the configuration to use as a cache filename.
sorted_ios_str=$(printf "%s\n" "${IO_CONNECTORS[@]}" | sort | tr '\n' '-' | sed 's/-$//')
CACHE_KEY="beam-${BEAM_VERSION}_runner-${BEAM_RUNNER}_ios-${sorted_ios_str}.jar"
CACHE_FILE="${CACHE_DIR}/${CACHE_KEY}"

# Check if a cached JAR already exists for this configuration.
if [ -f "${CACHE_FILE}" ]; then
  echo "✅ Found cached executable JAR. Skipping build."
  CP="${CACHE_FILE}"
else
  echo "🔎 No cache found. Building executable JAR (this might take a moment on first run)..."

  # --- Dynamic POM Generation ---
  POM_FILE="${WORK_DIR}/pom.xml"
  cat > "${POM_FILE}" << EOL
<project xmlns="http://maven.apache.org/POM/4.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sql-shell-runner</artifactId>
    <version>1.0</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-extensions-sql-jdbc</artifactId>
            <version>\${beam.version}</version>
        </dependency>
EOL
# Add IO and Runner dependencies
  for io in "${IO_CONNECTORS[@]}"; do
  echo "        <dependency><groupId>org.apache.beam</groupId><artifactId>beam-sdks-java-io-${io}</artifactId><version>\${beam.version}</version></dependency>" >> "${POM_FILE}"
  done
  RUNNER_ARTIFACT=""
  case "${BEAM_RUNNER}" in
    dataflow) RUNNER_ARTIFACT="beam-runners-google-cloud-dataflow-java" ;;
    direct) ;;
    *) echo "❌ Error: Unsupported runner '${BEAM_RUNNER}'." >&2; exit 1 ;;
  esac
  if [ -n "${RUNNER_ARTIFACT}" ]; then
  echo "        <dependency><groupId>org.apache.beam</groupId><artifactId>${RUNNER_ARTIFACT}</artifactId><version>\${beam.version}</version></dependency>" >> "${POM_FILE}"
  fi

# Complete the POM with the build section for the maven-shade-plugin
cat >> "${POM_FILE}" << EOL
    </dependencies>
    <properties>
      <beam.version>${BEAM_VERSION}</beam.version>
    </properties>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
EOL

  # Use `mvn package` to build the uber JAR.
  ${MAVEN_CMD} -f "${POM_FILE}" -q --batch-mode package

  UBER_JAR_PATH="${WORK_DIR}/target/beam-sql-shell-runner-1.0.jar"

  # Check if build was successful before caching
  if [ ! -f "${UBER_JAR_PATH}" ]; then
    echo "❌ Maven build failed. The uber JAR was not created." >&2
    exit 1
  fi

  # Copy the newly built JAR to our cache directory.
  cp "${UBER_JAR_PATH}" "${CACHE_FILE}"
  CP="${CACHE_FILE}"
  echo "✅ JAR built and cached for future use."
fi

# --- Launch Shell ---
echo "✅ Dependencies ready. Launching Beam SQL Shell..."
echo "----------------------------------------------------"

java -cp "${CP}" "${MAIN_CLASS}" "${SQLLINE_ARGS[@]}"

echo "----------------------------------------------------"
echo "👋 Exited Beam SQL Shell."
