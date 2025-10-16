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
DEFAULT_BEAM_VERSION="2.70.0"
MAIN_CLASS="org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine"
# Directory to store cached executable JAR files
CACHE_DIR="${HOME}/.beam/cache"

# Maven Wrapper Configuration
MAVEN_WRAPPER_VERSION="3.2.0"
MAVEN_VERSION="3.9.6"
MAVEN_WRAPPER_SCRIPT_URL="https://raw.githubusercontent.com/apache/maven-wrapper/refs/tags/maven-wrapper-${MAVEN_WRAPPER_VERSION}/maven-wrapper-distribution/src/resources/mvnw"
MAVEN_WRAPPER_JAR_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/${MAVEN_WRAPPER_VERSION}/maven-wrapper-${MAVEN_WRAPPER_VERSION}.jar"
MAVEN_DISTRIBUTION_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.zip"

# Maven Plugin Configuration
MAVEN_SHADE_PLUGIN_VERSION="3.5.1"
mkdir -p "${CACHE_DIR}"

# Create a temporary directory for our Maven project.
WORK_DIR=$(mktemp -d)

# Ensure cleanup on script exit
cleanup() {
  if [ -n "${WORK_DIR}" ] && [ -d "${WORK_DIR}" ]; then
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

# --- Helper Functions ---
# This function downloads the maven wrapper script and supporting files.
function setup_maven_wrapper() {
  local beam_dir="${HOME}/.beam"
  local maven_wrapper_dir="${beam_dir}/maven-wrapper"
  local mvnw_script="${maven_wrapper_dir}/mvnw"
  local wrapper_jar="${maven_wrapper_dir}/.mvn/wrapper/maven-wrapper.jar"
  local wrapper_props="${maven_wrapper_dir}/.mvn/wrapper/maven-wrapper.properties"

  # Check if Maven wrapper is already cached
  if [ -f "${mvnw_script}" ] && [ -f "${wrapper_jar}" ] && [ -f "${wrapper_props}" ]; then
    echo "🔧 Using cached Maven Wrapper from ${maven_wrapper_dir}"
    # Use the cached wrapper directly
    MAVEN_CMD="${mvnw_script}"
    return
  fi

  echo "🔧 Downloading Maven Wrapper for the first time..."
  mkdir -p "${maven_wrapper_dir}/.mvn/wrapper"

  # Create the properties file to specify a modern Maven version
  echo "distributionUrl=${MAVEN_DISTRIBUTION_URL}" > "${wrapper_props}"

  # Download the mvnw script and the wrapper JAR to cache directory
  curl -sSL -o "${mvnw_script}" "${MAVEN_WRAPPER_SCRIPT_URL}"
  curl -sSL -o "${wrapper_jar}" "${MAVEN_WRAPPER_JAR_URL}"

  # Make the wrapper script executable
  chmod +x "${mvnw_script}"

  echo "✅ Maven Wrapper cached in ${maven_wrapper_dir} for future use"
  # Use the cached wrapper directly
  MAVEN_CMD="${mvnw_script}"
}

function usage() {
  echo "Usage: $0 [--version <beam_version>] [--runner <runner_name>] [--io <io_connector>] [--list-versions] [--list-ios] [--list-runners] [--debug] [-h|--help]"
  echo ""
  echo "A self-contained launcher for the Apache Beam SQL Shell."
  echo ""
  echo "Options:"
  echo "  --version   Specify the Apache Beam version (default: ${DEFAULT_BEAM_VERSION})."
  echo "  --runner    Specify the Beam runner to use (default: direct)."
  echo "              Supported runners:"
  echo "                direct    - DirectRunner (runs locally, good for development)"
  echo "                dataflow  - DataflowRunner (runs on Google Cloud Dataflow)"
  echo "  --io        Specify an IO connector to include. Can be used multiple times."
  echo "              Available connectors: amazon-web-services2, amqp, azure,"
  echo "              azure-cosmos, cassandra, cdap, clickhouse, csv, debezium, elasticsearch,"
  echo "              google-ads, google-cloud-platform, hadoop-format, hbase, hcatalog, iceberg,"
  echo "              influxdb, jdbc, jms, json, kafka, kinesis, kudu, mongodb, mqtt, neo4j,"
  echo "              parquet, pulsar, rabbitmq, redis, singlestore, snowflake, solace, solr,"
  echo "              sparkreceiver, splunk, synthetic, thrift, tika, xml"
  echo "  --list-versions      List all available Beam versions from Maven Central and exit."
  echo "  --list-ios           List all available IO connectors from Maven Central and exit."
  echo "  --list-runners       List all available runners and exit."
  echo "  --debug     Enable debug mode (sets bash -x flag)."
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

# This function lists all available IO connectors by querying Maven Central.
function list_ios() {
  echo "🔎 Fetching available Apache Beam IO connectors from Maven Central..."
  local search_url="https://search.maven.org/solrsearch/select?q=g:org.apache.beam+AND+a:beam-sdks-java-io-*&rows=100&wt=json"

  if ! command -v curl &> /dev/null; then
    echo "❌ Error: 'curl' is required to fetch the IO connector list." >&2
    return 1
  fi

  # Fetch and parse the JSON response to extract IO connector names
  local ios
  ios=$(curl -sS "${search_url}" | \
    grep -o '"a":"beam-sdks-java-io-[^"]*"' | \
    sed 's/"a":"beam-sdks-java-io-\([^"]*\)"/\1/' | \
    grep -v -E '(tests?|expansion-service|parent|upgrade)' | \
    sort -u)

  if [ -z "${ios}" ]; then
    echo "❌ Could not retrieve IO connectors. Please check your internet connection or try again later." >&2
    echo "📋 Here are the known IO connectors (may not be complete):"
    echo "amazon-web-services2, amqp, azure, azure-cosmos, cassandra,"
    echo "cdap, clickhouse, csv, debezium, elasticsearch, google-ads, google-cloud-platform,"
    echo "hadoop-format, hbase, hcatalog, iceberg, influxdb, jdbc, jms, json, kafka, kinesis,"
    echo "kudu, mongodb, mqtt, neo4j, parquet, pulsar, rabbitmq, redis, singlestore, snowflake,"
    echo "solace, solr, sparkreceiver, splunk, synthetic, thrift, tika, xml"
    return 1
  fi

  echo "✅ Available IO connectors:"
  echo "${ios}" | tr '\n' ' ' | fold -s -w 80 | sed 's/^/  /'
}

# This function lists all available runners by querying Maven Central.
function list_runners() {
  echo "🚀 Fetching available Apache Beam runners for version ${BEAM_VERSION} from Maven Central..."
  local search_url="https://search.maven.org/solrsearch/select?q=g:org.apache.beam+AND+a:beam-runners-*+AND+v:${BEAM_VERSION}&rows=100&wt=json"

  if ! command -v curl &> /dev/null; then
    echo "❌ Error: 'curl' is required to fetch the runner list." >&2
    return 1
  fi

  # Fetch and parse the JSON response to extract runner names
  local runners
  runners=$(curl -sS "${search_url}" | \
    grep -o '"a":"beam-runners-[^"]*"' | \
    sed 's/"a":"beam-runners-\([^"]*\)"/\1/' | \
    grep -v -E '(tests?|parent|core-construction|core-java|extensions|job-server|legacy-worker|windmill|examples|experimental|orchestrator|java-fn-execution|java-job-service|gcp-gcemd|gcp-gcsproxy|local-java-core|portability-java|prism-java|reference-java)' | \
    sort -u)

  if [ -z "${runners}" ]; then
    echo "❌ Could not retrieve runners for version ${BEAM_VERSION}. Please check your internet connection or try again later." >&2
    echo "📋 Here are the known runners for recent Beam versions (may not be complete):"
    echo ""
    echo "  direct           - DirectRunner (runs locally, good for development)"
    echo "  dataflow         - DataflowRunner (runs on Google Cloud Dataflow)"
    echo "  flink            - FlinkRunner (runs on Apache Flink)"
    echo "  spark            - SparkRunner (runs on Apache Spark)"
    echo "  samza            - SamzaRunner (runs on Apache Samza)"
    echo "  jet              - JetRunner (runs on Hazelcast Jet)"
    echo "  twister2         - Twister2Runner (runs on Twister2)"
    echo ""
    echo "💡 Usage: ./beam-sql.sh --runner <runner_name>"
    echo "   Default: direct"
    echo "   Note: Only 'direct' and 'dataflow' are currently supported by this script."
    return 1
  fi

  echo "✅ Available runners for Beam ${BEAM_VERSION}:"
  echo ""
  
  # Process each runner and provide descriptions
  while IFS= read -r runner; do
    case "$runner" in
      "direct-java")
        echo "  direct           - DirectRunner"
        echo "                     Runs locally on your machine. Good for development and testing."
        ;;
      "google-cloud-dataflow-java")
        echo "  dataflow         - DataflowRunner" 
        echo "                     Runs on Google Cloud Dataflow for production workloads."
        ;;
      flink-*)
        local version=$(echo "$runner" | sed 's/flink-//')
        echo "  flink-${version} - FlinkRunner (Flink ${version})"
        echo "                     Runs on Apache Flink ${version} clusters."
        ;;
      flink_*)
        local version=$(echo "$runner" | sed 's/flink_//')
        echo "  flink-${version} - FlinkRunner (Flink ${version})"
        echo "                     Runs on Apache Flink ${version} clusters."
        ;;
      "spark")
        echo "  spark            - SparkRunner"
        echo "                     Runs on Apache Spark clusters."
        ;;
      "spark-3")
        echo "  spark-3          - SparkRunner (Spark 3.x)"
        echo "                     Runs on Apache Spark 3.x clusters."
        ;;
      "samza")
        echo "  samza            - SamzaRunner"
        echo "                     Runs on Apache Samza."
        ;;
      "jet")
        echo "  jet              - JetRunner"
        echo "                     Runs on Hazelcast Jet."
        ;;
      "twister2")
        echo "  twister2         - Twister2Runner"
        echo "                     Runs on Twister2."
        ;;
      "apex")
        echo "  apex             - ApexRunner"
        echo "                     Runs on Apache Apex."
        ;;
      "gearpump")
        echo "  gearpump         - GearpumpRunner"
        echo "                     Runs on Apache Gearpump."
        ;;
      "prism")
        echo "  prism            - PrismRunner"
        echo "                     Local runner for testing portable pipelines."
        ;;
      "reference")
        echo "  reference        - ReferenceRunner"
        echo "                     Reference implementation for testing."
        ;;
      "portability")
        echo "  portability      - PortabilityRunner"
        echo "                     For portable pipeline execution."
        ;;
      *)
        # For any other runners, clean up the name and show it
        local clean_name=$(echo "$runner" | sed -e 's/-java$//' -e 's/^gcp-//' -e 's/^local-//')
        echo "  ${clean_name}    - ${runner}"
        ;;
    esac
  done <<< "$runners"
  
  echo ""
  echo "💡 Usage: ./beam-sql.sh --runner <runner_name>"
  echo "   Default: direct"
  echo "   Note: This script currently supports 'direct' and 'dataflow' runners."
  echo "         Other runners may require additional setup and dependencies."
}


# --- Argument Parsing ---
BEAM_VERSION="${DEFAULT_BEAM_VERSION}"
IO_CONNECTORS=()
BEAM_RUNNER="direct"
SQLLINE_ARGS=()
DEBUG_MODE=false

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --version) BEAM_VERSION="$2"; shift ;;
    --runner) BEAM_RUNNER=$(echo "$2" | tr '[:upper:]' '[:lower:]'); shift ;;
    --io) IO_CONNECTORS+=("$2"); shift ;;
    --list-versions) list_versions; exit 0 ;;
    --list-ios) list_ios; exit 0 ;;
    --list-runners) list_runners; exit 0 ;;
    --debug) DEBUG_MODE=true ;;
    -h|--help) usage ;;
    *) SQLLINE_ARGS+=("$1") ;;
  esac
  shift
done

# Enable debug mode if requested
if [ "${DEBUG_MODE}" = true ]; then
  set -x
fi

# --- Prerequisite Check ---
# Java is always required.
if ! command -v java &> /dev/null; then
  echo "❌ Error: 'java' command not found. It is required to run the application." >&2
  exit 1
fi

# Curl is required for Maven wrapper setup.
if ! command -v curl &> /dev/null; then
  echo "❌ Error: 'curl' command not found. It is required to download the Maven wrapper." >&2
  exit 1
fi

setup_maven_wrapper

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
                <version>${MAVEN_SHADE_PLUGIN_VERSION}</version>
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
  echo "💾 JAR built and cached for future use."
fi

# --- Launch Shell ---
echo "✅ Dependencies ready. Launching Beam SQL Shell..."
echo "----------------------------------------------------"

java -cp "${CP}" "${MAIN_CLASS}" "${SQLLINE_ARGS[@]}"

echo "----------------------------------------------------"
echo "👋 Exited Beam SQL Shell."
