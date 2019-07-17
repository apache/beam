/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import groovy.json.JsonOutput
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.FileTree
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.plugins.quality.FindBugs
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.testing.jacoco.tasks.JacocoReport

/**
 * This plugin adds methods to configure a module with Beam's defaults, called "natures".
 *
 * <p>The natures available:
 *
 * <ul>
 *   <li>Java   - Configures plugins commonly found in Java projects
 *   <li>Go     - Configures plugins commonly found in Go projects
 *   <li>Docker - Configures plugins commonly used to build Docker containers
 *   <li>Grpc   - Configures plugins commonly used to generate source from protos
 *   <li>Avro   - Configures plugins commonly used to generate source from Avro specifications
 * </ul>
 *
 * <p>For example, see applyJavaNature.
 */
class BeamModulePlugin implements Plugin<Project> {

  /** A class defining the set of configurable properties accepted by applyJavaNature. */
  class JavaNatureConfiguration {
    /** Controls the JDK source language and target compatibility. */
    double javaVersion = 1.8

    /** Controls whether the findbugs plugin is enabled and configured. */
    boolean enableFindbugs = true

    /**
     * List of additional lint warnings to disable.
     * In addition, defaultLintSuppressions defined below
     * will be applied to all projects.
     */
    List<String> disableLintWarnings = []

    /** Controls whether tests are run with shadowJar. */
    boolean testShadowJar = false

    /**
     * Controls whether the shadow jar is validated to not contain any classes outside the org.apache.beam namespace.
     * This protects artifact jars from leaking dependencies classes causing conflicts for users.
     *
     * Note that this can be disabled for subprojects that produce application artifacts that are not intended to
     * be depended on by users.
     */
    boolean validateShadowJar = true

    /**
     * The shadowJar / shadowTestJar tasks execute the following closure to configure themselves.
     * Users can compose their closure with the default closure via:
     * DEFAULT_SHADOW_CLOSURE << {
     *   dependencies {
     *     include(...)
     *   }
     *   relocate(...)
     * }
     */
    Closure shadowClosure;

    /** Controls whether this project is published to Maven. */
    boolean publish = true
  }

  // A class defining the set of configurable properties for createJavaExamplesArchetypeValidationTask
  class JavaExamplesArchetypeValidationConfiguration {
    // Type [Quickstart, MobileGaming] for the postrelease validation is required.
    // Used both for the test name run${type}Java${runner}
    // and also for the script name, ${type}-java-${runner}.toLowerCase().
    String type

    // runner [Direct, Dataflow, Spark, Flink, FlinkLocal, Apex]
    String runner

    // gcpProject sets the gcpProject argument when executing examples.
    String gcpProject

    // gcsBucket sets the gcsProject argument when executing examples.
    String gcsBucket

    // bqDataset sets the BigQuery Dataset when executing mobile-gaming examples
    String bqDataset

    // pubsubTopic sets topics when executing streaming pipelines
    String pubsubTopic
  }

  // Reads and contains all necessary performance test parameters
  class JavaPerformanceTestConfiguration {

    /* Optional properties (set only if needed in your case): */

    // Path to PerfKitBenchmarker application (pkb.py).
    // It is only required when running Performance Tests with PerfKitBenchmarker
    String pkbLocation = System.getProperty('pkbLocation')

    // Data Processing Backend's log level.
    String logLevel = System.getProperty('logLevel', 'INFO')

    // Path to gradle binary.
    String gradleBinary = System.getProperty('gradleBinary', './gradlew')

    // If benchmark is official or not.
    // Official benchmark results are meant to be displayed on PerfKitExplorer dashboards.
    String isOfficial = System.getProperty('official', 'false')

    // Specifies names of benchmarks to be run by PerfKitBenchmarker.
    String benchmarks = System.getProperty('benchmarks', 'beam_integration_benchmark')

    // If beam is not "prebuilt" then PerfKitBenchmarker runs the build task before running the tests.
    String beamPrebuilt = System.getProperty('beamPrebuilt', 'true')

    // Beam's sdk to be used by PerfKitBenchmarker.
    String beamSdk = System.getProperty('beamSdk', 'java')

    // Timeout (in seconds) after which PerfKitBenchmarker will stop executing the benchmark (and will fail).
    String timeout = System.getProperty('itTimeout', '1200')

    // Path to kubernetes configuration file.
    String kubeconfig = System.getProperty('kubeconfig', System.getProperty('user.home') + '/.kube/config')

    // Path to kubernetes executable.
    String kubectl = System.getProperty('kubectl', 'kubectl')

    // Paths to files with kubernetes infrastructure to setup before the test runs.
    // PerfKitBenchmarker will have trouble reading 'null' path. It expects empty string if no scripts are expected.
    String kubernetesScripts = System.getProperty('kubernetesScripts', '')

    // Path to file with 'dynamic' and 'static' pipeline options.
    // that will be appended by PerfKitBenchmarker to the test running command.
    // PerfKitBenchmarker will have trouble reading 'null' path. It expects empty string if no config file is expected.
    String optionsConfigFile = System.getProperty('beamITOptions', '')

    // Any additional properties to be appended to benchmark execution command.
    String extraProperties = System.getProperty('beamExtraProperties', '')

    // Runner which will be used for running the tests. Possible values: dataflow/direct.
    // PerfKitBenchmarker will have trouble reading 'null' value. It expects empty string if no config file is expected.
    String runner = System.getProperty('integrationTestRunner', '')

    // Filesystem which will be used for running the tests. Possible values: hdfs.
    // if not specified runner's local filesystem will be used.
    String filesystem = System.getProperty('filesystem')

    /* Always required properties: */

    // Pipeline options to be used by the tested pipeline.
    String integrationTestPipelineOptions = System.getProperty('integrationTestPipelineOptions')

    // Fully qualified name of the test to be run, eg:
    // 'org.apache.beam.sdks.java.io.jdbc.JdbcIOIT'.
    String integrationTest = System.getProperty('integrationTest')

    // Relative path to module where the test is, eg. 'sdks/java/io/jdbc.
    String itModule = System.getProperty('itModule')
  }

  // A class defining the set of configurable properties accepted by containerImageName.
  class ContainerImageNameConfiguration {
    String root = null // Sets the docker repository root (optional).
    String name = null // Sets the short container image name, such as "go" (required).
    String tag = null // Sets the image tag (optional).
  }

  // A class defining the configuration for PortableValidatesRunner.
  class PortableValidatesRunnerConfiguration {
    // Task name for validate runner case.
    String name = 'validatesPortableRunner'
    // Fully qualified JobServerClass name to use.
    String jobServerDriver
    // A string representing the jobServer Configuration.
    String jobServerConfig
    // Categories for tests to run.
    Closure testCategories = {
      includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      excludeCategories 'org.apache.beam.sdk.testing.FlattenWithHeterogeneousCoders'
      excludeCategories 'org.apache.beam.sdk.testing.LargeKeys$Above100MB'
      excludeCategories 'org.apache.beam.sdk.testing.UsesCommittedMetrics'
      excludeCategories 'org.apache.beam.sdk.testing.UsesGaugeMetrics'
      excludeCategories 'org.apache.beam.sdk.testing.UsesDistributionMetrics'
      excludeCategories 'org.apache.beam.sdk.testing.UsesAttemptedMetrics'
      excludeCategories 'org.apache.beam.sdk.testing.UsesTimersInParDo'
      excludeCategories 'org.apache.beam.sdk.testing.UsesTestStream'
    }
    // Configuration for the classpath when running the test.
    Configuration testClasspathConfiguration
  }

  def isRelease(Project project) {
    return project.hasProperty('isRelease')
  }

  void apply(Project project) {

    /** ***********************************************************************************************/
    // Apply common properties/repositories and tasks to all projects.

    project.group = 'org.apache.beam'

    // Automatically use the official release version if we are performing a release
    // otherwise append '-SNAPSHOT'
    project.version = '2.7.0'
    if (!isRelease(project)) {
      project.version += '-SNAPSHOT'
    }

    project.repositories {
      maven { url project.offlineRepositoryRoot }

      // To run gradle in offline mode, one must first invoke
      // 'updateOfflineRepository' to create an offline repo
      // inside the root project directory. See the application
      // of the offline repo plugin within build_rules.gradle
      // for further details.
      if (project.gradle.startParameter.isOffline()) {
        return
      }

      mavenLocal()
      mavenCentral()
      jcenter()

      // Spring for resolving pentaho dependency.
      maven { url "https://repo.spring.io/plugins-release/" }

      // Release staging repository
      maven { url "https://oss.sonatype.org/content/repositories/staging/" }

      // Apache nightly snapshots
      maven { url "https://repository.apache.org/snapshots" }

      // Apache release snapshots
      maven { url "https://repository.apache.org/content/repositories/releases" }
    }

    // Apply a plugin which enables configuring projects imported into Intellij.
    project.apply plugin: "idea"

    // Provide code coverage
    // TODO: Should this only apply to Java projects?
    project.apply plugin: "jacoco"
    project.gradle.taskGraph.whenReady { graph ->
      // Disable jacoco unless report requested such that task outputs can be properly cached.
      // https://discuss.gradle.org/t/do-not-cache-if-condition-matched-jacoco-agent-configured-with-append-true-satisfied/23504
      def enabled = graph.allTasks.any { it instanceof JacocoReport }
      project.tasks.withType(Test) { jacoco.enabled = enabled }
    }

    // Apply a plugin which provides tasks for dependency / property / task reports.
    // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
    // for further details. This is typically very useful to look at the "htmlDependencyReport"
    // when attempting to resolve dependency issues.
    project.apply plugin: "project-report"

    /** ***********************************************************************************************/
    // Define and export a map dependencies shared across multiple sub-projects.
    //
    // Example usage:
    // configuration {
    //   shadow library.java.avro
    //   shadowTest library.java.junit
    // }

    // These versions are defined here because they represent
    // a dependency version which should match across multiple
    // Maven artifacts.
    def generated_grpc_beta_version = "0.19.0"
    def generated_grpc_ga_version = "1.18.0"
    def google_cloud_bigdataoss_version = "1.9.13"
    def bigtable_version = "1.4.0"
    def google_clients_version = "1.23.0"
    def google_auth_version = "0.10.0"
    def grpc_version = "1.13.1"
    def protobuf_version = "3.6.0"
    def guava_version = "20.0"
    def netty_version = "4.1.25.Final"
    def proto_google_common_protos_version = "1.12.0"
    def hamcrest_version = "1.3"
    def hadoop_version = "2.7.3"
    def jackson_version = "2.9.5"
    def spark_version = "2.3.1"
    def apex_core_version = "3.7.0"
    def apex_malhar_version = "3.4.0"
    def postgres_version = "42.2.2"
    def jaxb_api_version = "2.2.12"
    def kafka_version = "1.0.0"
    def quickcheck_version = "0.8"
    def google_cloud_core_version = "1.36.0"

    // A map of maps containing common libraries used per language. To use:
    // dependencies {
    //   shadow library.java.slf4j_api
    // }
    project.ext.library = [
      java : [
        activemq_amqp                               : "org.apache.activemq:activemq-amqp:5.13.1",
        activemq_broker                             : "org.apache.activemq:activemq-broker:5.13.1",
        activemq_client                             : "org.apache.activemq:activemq-client:5.13.1",
        activemq_jaas                               : "org.apache.activemq:activemq-jaas:5.13.1",
        activemq_junit                              : "org.apache.activemq.tooling:activemq-junit:5.13.1",
        activemq_kahadb_store                       : "org.apache.activemq:activemq-kahadb-store:5.13.1",
        activemq_mqtt                               : "org.apache.activemq:activemq-mqtt:5.13.1",
        apex_common                                 : "org.apache.apex:apex-common:$apex_core_version",
        apex_engine                                 : "org.apache.apex:apex-engine:$apex_core_version",
        args4j                                      : "args4j:args4j:2.33",
        avro                                        : "org.apache.avro:avro:1.8.2",
        bigdataoss_gcsio                            : "com.google.cloud.bigdataoss:gcsio:$google_cloud_bigdataoss_version",
        bigdataoss_util                             : "com.google.cloud.bigdataoss:util:$google_cloud_bigdataoss_version",
        bigtable_client_core                        : "com.google.cloud.bigtable:bigtable-client-core:$bigtable_version",
        bigtable_protos                             : "com.google.api.grpc:grpc-google-cloud-bigtable-v2:$generated_grpc_beta_version",
        byte_buddy                                  : "net.bytebuddy:byte-buddy:1.8.11",
        commons_compress                            : "org.apache.commons:commons-compress:1.16.1",
        commons_csv                                 : "org.apache.commons:commons-csv:1.4",
        commons_io_1x                               : "commons-io:commons-io:1.3.2",
        commons_io_2x                               : "commons-io:commons-io:2.5",
        commons_lang3                               : "org.apache.commons:commons-lang3:3.6",
        commons_math3                               : "org.apache.commons:commons-math3:3.6.1",
        datastore_v1_proto_client                   : "com.google.cloud.datastore:datastore-v1-proto-client:1.6.0",
        datastore_v1_protos                         : "com.google.api.grpc:proto-google-cloud-datastore-v1:$generated_grpc_beta_version",
        error_prone_annotations                     : "com.google.errorprone:error_prone_annotations:2.0.15",
        gax_grpc                                    : "com.google.api:gax-grpc:1.29.0",
        google_api_client                           : "com.google.api-client:google-api-client:$google_clients_version",
        google_api_client_jackson2                  : "com.google.api-client:google-api-client-jackson2:$google_clients_version",
        google_api_client_java6                     : "com.google.api-client:google-api-client-java6:$google_clients_version",
        google_api_common                           : "com.google.api:api-common:1.6.0",
        google_api_services_bigquery                : "com.google.apis:google-api-services-bigquery:v2-rev374-$google_clients_version",
        google_api_services_clouddebugger           : "com.google.apis:google-api-services-clouddebugger:v2-rev233-$google_clients_version",
        google_api_services_cloudresourcemanager    : "com.google.apis:google-api-services-cloudresourcemanager:v1-rev477-$google_clients_version",
        google_api_services_dataflow                : "com.google.apis:google-api-services-dataflow:v1b3-rev221-$google_clients_version",
        google_api_services_pubsub                  : "com.google.apis:google-api-services-pubsub:v1-rev382-$google_clients_version",
        google_api_services_storage                 : "com.google.apis:google-api-services-storage:v1-rev124-$google_clients_version",
        google_auth_library_credentials             : "com.google.auth:google-auth-library-credentials:$google_auth_version",
        google_auth_library_oauth2_http             : "com.google.auth:google-auth-library-oauth2-http:$google_auth_version",
        google_cloud_core                           : "com.google.cloud:google-cloud-core:$google_cloud_core_version",
        google_cloud_core_grpc                      : "com.google.cloud:google-cloud-core-grpc:$google_cloud_core_version",
        google_cloud_dataflow_java_proto_library_all: "com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:0.5.160304",
        google_cloud_spanner                        : "com.google.cloud:google-cloud-spanner:0.54.0-beta",
        google_http_client                          : "com.google.http-client:google-http-client:$google_clients_version",
        google_http_client_jackson                  : "com.google.http-client:google-http-client-jackson:$google_clients_version",
        google_http_client_jackson2                 : "com.google.http-client:google-http-client-jackson2:$google_clients_version",
        google_http_client_protobuf                 : "com.google.http-client:google-http-client-protobuf:$google_clients_version",
        google_oauth_client                         : "com.google.oauth-client:google-oauth-client:$google_clients_version",
        google_oauth_client_java6                   : "com.google.oauth-client:google-oauth-client-java6:$google_clients_version",
        grpc_all                                    : "io.grpc:grpc-all:$grpc_version",
        grpc_auth                                   : "io.grpc:grpc-auth:$grpc_version",
        grpc_core                                   : "io.grpc:grpc-core:$grpc_version",
        grpc_google_cloud_pubsub_v1                 : "com.google.api.grpc:grpc-google-cloud-pubsub-v1:$generated_grpc_ga_version",
        grpc_protobuf                               : "io.grpc:grpc-protobuf:$grpc_version",
        grpc_protobuf_lite                          : "io.grpc:grpc-protobuf-lite:$grpc_version",
        grpc_netty                                  : "io.grpc:grpc-netty:$grpc_version",
        grpc_stub                                   : "io.grpc:grpc-stub:$grpc_version",
        guava                                       : "com.google.guava:guava:$guava_version",
        guava_testlib                               : "com.google.guava:guava-testlib:$guava_version",
        hadoop_client                               : "org.apache.hadoop:hadoop-client:$hadoop_version",
        hadoop_common                               : "org.apache.hadoop:hadoop-common:$hadoop_version",
        hadoop_mapreduce_client_core                : "org.apache.hadoop:hadoop-mapreduce-client-core:$hadoop_version",
        hadoop_minicluster                          : "org.apache.hadoop:hadoop-minicluster:$hadoop_version",
        hadoop_hdfs                                 : "org.apache.hadoop:hadoop-hdfs:$hadoop_version",
        hadoop_hdfs_tests                           : "org.apache.hadoop:hadoop-hdfs:$hadoop_version:tests",
        hamcrest_core                               : "org.hamcrest:hamcrest-core:$hamcrest_version",
        hamcrest_library                            : "org.hamcrest:hamcrest-library:$hamcrest_version",
        jackson_annotations                         : "com.fasterxml.jackson.core:jackson-annotations:$jackson_version",
        jackson_core                                : "com.fasterxml.jackson.core:jackson-core:$jackson_version",
        jackson_databind                            : "com.fasterxml.jackson.core:jackson-databind:$jackson_version",
        jackson_dataformat_cbor                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jackson_version",
        jackson_dataformat_yaml                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jackson_version",
        jackson_datatype_joda                       : "com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson_version",
        jackson_module_scala                        : "com.fasterxml.jackson.module:jackson-module-scala_2.11:$jackson_version",
        jaxb_api                                    : "javax.xml.bind:jaxb-api:$jaxb_api_version",
        joda_time                                   : "joda-time:joda-time:2.4",
        junit                                       : "junit:junit:4.12",
        kafka_2_11                                  : "org.apache.kafka:kafka_2.11:$kafka_version",
        kafka_clients                               : "org.apache.kafka:kafka-clients:$kafka_version",
        malhar_library                              : "org.apache.apex:malhar-library:$apex_malhar_version",
        mockito_core                                : "org.mockito:mockito-core:1.10.19",
        netty_handler                               : "io.netty:netty-handler:$netty_version",
        netty_tcnative_boringssl_static             : "io.netty:netty-tcnative-boringssl-static:2.0.8.Final",
        netty_transport_native_epoll                : "io.netty:netty-transport-native-epoll:$netty_version",
        postgres                                    : "org.postgresql:postgresql:$postgres_version",
        powermock                                   : "org.powermock:powermock-mockito-release-full:1.6.4",
        protobuf_java                               : "com.google.protobuf:protobuf-java:$protobuf_version",
        protobuf_java_util                          : "com.google.protobuf:protobuf-java-util:$protobuf_version",
        proto_google_cloud_pubsub_v1                : "com.google.api.grpc:proto-google-cloud-pubsub-v1:$generated_grpc_ga_version",
        proto_google_cloud_spanner_admin_database_v1: "com.google.api.grpc:proto-google-cloud-spanner-admin-database-v1:$generated_grpc_beta_version",
        proto_google_common_protos                  : "com.google.api.grpc:proto-google-common-protos:$proto_google_common_protos_version",
        slf4j_api                                   : "org.slf4j:slf4j-api:1.7.25",
        slf4j_simple                                : "org.slf4j:slf4j-simple:1.7.25",
        slf4j_jdk14                                 : "org.slf4j:slf4j-jdk14:1.7.25",
        slf4j_log4j12                               : "org.slf4j:slf4j-log4j12:1.7.25",
        snappy_java                                 : "org.xerial.snappy:snappy-java:1.1.4",
        spark_core                                  : "org.apache.spark:spark-core_2.11:$spark_version",
        spark_network_common                        : "org.apache.spark:spark-network-common_2.11:$spark_version",
        spark_streaming                             : "org.apache.spark:spark-streaming_2.11:$spark_version",
        stax2_api                                   : "org.codehaus.woodstox:stax2-api:3.1.4",
        woodstox_core_asl                           : "org.codehaus.woodstox:woodstox-core-asl:4.4.1",
        quickcheck_core                             : "com.pholser:junit-quickcheck-core:$quickcheck_version",
      ],
      // For generating pom.xml from archetypes.
      maven: [
        maven_compiler_plugin: "maven-plugins:maven-compiler-plugin:3.7.0",
        maven_exec_plugin    : "maven-plugins:maven-exec-plugin:1.6.0",
        maven_jar_plugin     : "maven-plugins:maven-jar-plugin:3.0.2",
        maven_shade_plugin   : "maven-plugins:maven-shade-plugin:3.1.0",
        maven_surefire_plugin: "maven-plugins:maven-surefire-plugin:2.21.0",
      ],
    ]

    /** ***********************************************************************************************/

    // Returns a string representing the relocated path to be used with the shadow plugin when
    // given a suffix such as "com.google.common".
    project.ext.getJavaRelocatedPath = { String suffix ->
      return ("org.apache.beam.repackaged."
              + project.name.replace("-", "_")
              + "."
              + suffix)
    }

    // By default if there is at least one include rule then all included dependencies must be specified.
    // This overrides the default behavior of include all if no includes are specified.
    // See details here:
    // https://github.com/johnrengelman/shadow/blob/98191096a94674245c7b3e63975df9e14f67074e/src/main/groovy/com/github/jengelman/gradle/plugins/shadow/internal/DefaultDependencyFilter.groovy#L123
    project.ext.DEFAULT_SHADOW_CLOSURE = {
      dependencies {
        include(dependency(project.library.java.guava))
      }
      // guava uses the com.google.common and com.google.thirdparty package namespaces
      relocate("com.google.common", project.getJavaRelocatedPath("com.google.common")) {
        // com.google.common is too generic, need to exclude guava-testlib
        exclude "com.google.common.collect.testing.**"
        exclude "com.google.common.escape.testing.**"
        exclude "com.google.common.testing.**"
        exclude "com.google.common.util.concurrent.testing.**"
      }
      relocate "com.google.thirdparty", project.getJavaRelocatedPath("com.google.thirdparty")
    }

    // Configures a project with a default set of plugins that should apply to all Java projects.
    //
    // Users should invoke this method using Groovy map syntax. For example:
    // applyJavaNature(javaVersion: 1.8)
    //
    // See JavaNatureConfiguration for the set of accepted properties.
    //
    // The following plugins are enabled:
    //  * java
    //  * maven
    //  * net.ltgt.apt (plugin to configure annotation processing tool)
    //  * propdeps (provide optional and provided dependency configurations)
    //  * propdeps-maven
    //  * checkstyle
    //  * findbugs
    //  * shadow
    //  * com.diffplug.gradle.spotless (code style plugin)
    //
    // Dependency Management for Java Projects
    // ---------------------------------------
    //
    // By default, the shadow plugin is enabled to perform shading of commonly found dependencies.
    // Because of this it is important that dependencies are added to the correct configuration.
    // Dependencies should fall into one of these four configurations:
    //  * compile     - Required during compilation or runtime of the main source set.
    //                  This configuration represents all dependencies that much also be shaded away
    //                  otherwise the generated Maven pom will be missing this dependency.
    //  * shadow      - Required during compilation or runtime of the main source set.
    //                  Will become a runtime dependency of the generated Maven pom.
    //  * testCompile - Required during compilation or runtime of the test source set.
    //                  This must be shaded away in the shaded test jar.
    //  * shadowTest  - Required during compilation or runtime of the test source set.
    //                  TODO: Figure out whether this should be a test scope dependency
    //                  of the generated Maven pom.
    //
    // When creating a cross-project dependency between two Java projects, one should only rely on the shaded configurations.
    // This allows for compilation/test execution to occur against the final artifact that will be provided to users.
    // This is by done by referencing the "shadow" or "shadowTest" configuration as so:
    //   dependencies {
    //     shadow project(path: "other:java:project1", configuration: "shadow")
    //     shadowTest project(path: "other:java:project2", configuration: "shadowTest")
    //   }
    // This will ensure the correct set of transitive dependencies from those projects are correctly added to the
    // main and test source set runtimes.

    project.ext.applyJavaNature = {
      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      JavaNatureConfiguration configuration = it ? it as JavaNatureConfiguration : new JavaNatureConfiguration()
      if (!configuration.shadowClosure) {
        configuration.shadowClosure = project.DEFAULT_SHADOW_CLOSURE
      }

      project.apply plugin: "java"

      // Configure the Java compiler source language and target compatibility levels. Also ensure that
      // we configure the Java compiler to use UTF-8.
      project.sourceCompatibility = configuration.javaVersion
      project.targetCompatibility = configuration.javaVersion

      def defaultLintSuppressions = [
        'options',
        'cast',
        'deprecation',
        'processing',
        'rawtypes',
        'serial',
        'try',
        'unchecked',
        'varargs',
      ]

      project.tasks.withType(JavaCompile) {
        options.encoding = "UTF-8"
        options.compilerArgs += ([
          '-parameters',
          '-Xlint:all',
          '-Werror',
          '-XepDisableWarningsInGeneratedCode',
          '-XepExcludedPaths:(.*/)?(build/generated.*avro-java|build/generated)/.*',
          '-Xep:MutableConstantField:OFF' // Guava's immutable collections cannot appear on API surface.
        ]
        + (defaultLintSuppressions + configuration.disableLintWarnings).collect { "-Xlint:-${it}" })
      }

      // Configure the default test tasks set of tests executed
      // to match the equivalent set that is executed by the maven-surefire-plugin.
      // See http://maven.apache.org/components/surefire/maven-surefire-plugin/test-mojo.html
      project.test {
        include "**/Test*.class"
        include "**/*Test.class"
        include "**/*Tests.class"
        include "**/*TestCase.class"
      }

      // Configure all test tasks to use JUnit
      project.tasks.withType(Test) { useJUnit {} }

      // Ensure that tests are packaged and part of the artifact set.
      project.task('packageTests', type: Jar) {
        classifier = 'tests'
        from project.sourceSets.test.output
      }
      project.artifacts.archives project.packageTests

      // Apply a plugin which provides the 'updateOfflineRepository' task that creates an offline
      // repository. This offline repository satisfies all Gradle build dependencies and Java
      // project dependencies. The offline repository is placed within $rootDir/offline-repo
      // but can be overridden by specifying '-PofflineRepositoryRoot=/path/to/repo'.
      // Note that parallel build must be disabled when executing 'updateOfflineRepository'
      // by specifying '--no-parallel', see
      // https://github.com/mdietrichstein/gradle-offline-dependencies-plugin/issues/3
      project.apply plugin: "io.pry.gradle.offline_dependencies"
      project.offlineDependencies {
        repositories {
          mavenLocal()
          mavenCentral()
          jcenter()
          maven { url "https://plugins.gradle.org/m2/" }
          maven { url "http://repo.spring.io/plugins-release" }
          maven { url project.offlineRepositoryRoot }
        }
        includeSources = false
        includeJavadocs = false
        includeIvyXmls = false
      }

      // Configures annotation processing for commonly used annotation processors
      // across all Java projects.
      project.apply plugin: "net.ltgt.apt"
      project.dependencies {
        // Note that these plugins specifically use the compileOnly and testCompileOnly
        // configurations because they are never required to be shaded or become a
        // dependency of the output.
        def auto_value = "com.google.auto.value:auto-value:1.5.3"
        def auto_service = "com.google.auto.service:auto-service:1.0-rc2"

        compileOnly auto_value
        apt auto_value
        testCompileOnly auto_value
        testApt auto_value

        compileOnly auto_service
        apt auto_service
        testCompileOnly auto_service
        testApt auto_service

        // These dependencies are needed to avoid error-prone warnings on package-info.java files,
        // also to include the annotations to suppress warnings.
        //
        // findbugs-annotations artifact is licensed under LGPL and cannot be included in the
        // Apache Beam distribution, but may be relied on during build.
        // See: https://www.apache.org/legal/resolved.html#prohibited
        def findbugs_annotations = "com.google.code.findbugs:annotations:3.0.1"
        compileOnly findbugs_annotations
        apt findbugs_annotations
        testCompileOnly findbugs_annotations
        testApt findbugs_annotations
      }

      // Add the optional and provided configurations for dependencies
      // TODO: Either remove these plugins and find another way to generate the Maven poms
      // with the correct dependency scopes configured.
      project.apply plugin: 'propdeps'
      project.apply plugin: 'propdeps-maven'

      // Configures a checkstyle plugin enforcing a set of rules and also allows for a set of
      // suppressions.
      project.apply plugin: 'checkstyle'
      project.tasks.withType(Checkstyle) {
        configFile = project.project(":").file("sdks/java/build-tools/src/main/resources/beam/checkstyle.xml")
        configProperties = ["checkstyle.suppressions.file": project.project(":").file("sdks/java/build-tools/src/main/resources/beam/suppressions.xml")]
        showViolations = true
        maxErrors = 0
      }
      project.checkstyle { toolVersion = "8.7" }

      // Ensure check runs javadoc
      project.check.dependsOn project.javadoc

      // Apply the eclipse and apt-eclipse plugins.  This adds the "eclipse" task and
      // connects the apt-eclipse plugin to update the eclipse project files
      // with the instructions needed to run apt within eclipse to handle the AutoValue
      // and additional annotations
      project.apply plugin: 'eclipse'
      project.apply plugin: "net.ltgt.apt-eclipse"

      // Enables a plugin which can apply code formatting to source.
      // TODO(https://issues.apache.org/jira/browse/BEAM-4394): Should this plugin be enabled for all projects?
      project.apply plugin: "com.diffplug.gradle.spotless"
      project.spotless { java { googleJavaFormat() } }

      // Enables a plugin which performs code analysis for common bugs.
      // This plugin is configured to only analyze the "main" source set.
      if (configuration.enableFindbugs) {
        project.apply plugin: 'findbugs'
        project.findbugs {
          excludeFilter = project.rootProject.file('sdks/java/build-tools/src/main/resources/beam/findbugs-filter.xml')
          sourceSets = [sourceSets.main]
        }
        project.tasks.withType(FindBugs) {
          reports {
            html.enabled = true
            xml.enabled = false
          }
        }
      }

      // Enable errorprone static analysis
      project.apply plugin: 'net.ltgt.errorprone'

      project.configurations.errorprone { resolutionStrategy.force 'com.google.errorprone:error_prone_core:2.3.1' }

      // Enables a plugin which can perform shading of classes. See the general comments
      // above about dependency management for Java projects and how the shadow plugin
      // is expected to be used for the different Gradle configurations.
      //
      // TODO: Enforce all relocations are always performed to:
      // getJavaRelocatedPath(package_suffix) where package_suffix is something like "com.google.commmon"
      project.apply plugin: 'com.github.johnrengelman.shadow'

      // Create a new configuration 'shadowTest' like 'shadow' for the test scope
      project.configurations {
        shadow { description = "Dependencies for shaded source set 'main'" }
        compile.extendsFrom shadow
        shadowTest {
          description = "Dependencies for shaded source set 'test'"
          extendsFrom shadow
        }
        testCompile.extendsFrom shadowTest
      }

      project.jar {
        classifier = "unshaded"
        zip64 true
      }

      // Always configure the shadowJar classifier and merge service files.
      project.shadowJar({
        classifier = null
        mergeServiceFiles()
        zip64 true
        into("META-INF/") {
          from "${project.rootProject.projectDir}/LICENSE"
          from "${project.rootProject.projectDir}/NOTICE"
        }
      } << configuration.shadowClosure)

      // Always configure the shadowTestJar classifier and merge service files.
      project.task('shadowTestJar', type: ShadowJar, {
        group = "Shadow"
        description = "Create a combined JAR of project and test dependencies"
        classifier = "tests"
        from project.sourceSets.test.output
        configurations = [
          project.configurations.testRuntime
        ]
        zip64 true
        exclude "META-INF/INDEX.LIST"
        exclude "META-INF/*.SF"
        exclude "META-INF/*.DSA"
        exclude "META-INF/*.RSA"
      } << configuration.shadowClosure)

      // Ensure that shaded jar and test-jar are part of the their own configuration artifact sets
      project.artifacts.shadow project.shadowJar
      project.artifacts.shadowTest project.shadowTestJar

      if (configuration.testShadowJar) {
        // Use a configuration and dependency set which represents the execution classpath using shaded artifacts for tests.
        project.configurations { shadowTestRuntimeClasspath }

        project.dependencies {
          shadowTestRuntimeClasspath it.project(path: project.path, configuration: "shadowTest")
          shadowTestRuntimeClasspath it.project(path: project.path, configuration: "provided")
        }

        project.test { classpath = project.configurations.shadowTestRuntimeClasspath }
      }

      if (configuration.validateShadowJar) {
        project.task('validateShadedJarDoesntLeakNonOrgApacheBeamClasses', dependsOn: 'shadowJar') {
          inputs.files project.configurations.shadow.artifacts.files
          doLast {
            project.configurations.shadow.artifacts.files.each {
              FileTree exposedClasses = project.zipTree(it).matching {
                include "**/*.class"
                exclude "org/apache/beam/**"
              }
              if (exposedClasses.files) {
                throw new GradleException("$it exposed classes outside of org.apache.beam namespace: ${exposedClasses.files}")
              }
            }
          }
        }
        project.tasks.check.dependsOn project.tasks.validateShadedJarDoesntLeakNonOrgApacheBeamClasses
      }

      if ((isRelease(project) || project.hasProperty('publishing')) &&
      configuration.publish) {
        project.apply plugin: "maven-publish"

        // Create a task which emulates the maven-archiver plugin in generating a
        // pom.properties file.
        def pomPropertiesFile = "${project.buildDir}/publications/mavenJava/pom.properties"
        project.task('generatePomPropertiesFileForMavenJavaPublication') {
          outputs.file "${pomPropertiesFile}"
          doLast {
            new File("${pomPropertiesFile}").text =
                    """version=${project.version}
groupId=${project.group}
artifactId=${project.name}
"""
          }
        }

        // Have the shaded include both the generate pom.xml and its properties file
        // emulating the behavior of the maven-archiver plugin.
        project.shadowJar {
          def pomFile = "${project.buildDir}/publications/mavenJava/pom-default.xml"

          // Validate that the artifacts exist before copying them into the jar.
          doFirst {
            if (!project.file("${pomFile}").exists()) {
              throw new GradleException("Expected ${pomFile} to have been generated by the 'generatePomFileForMavenJavaPublication' task.")
            }
            if (!project.file("${pomPropertiesFile}").exists()) {
              throw new GradleException("Expected ${pomPropertiesFile} to have been generated by the 'generatePomPropertiesFileForMavenJavaPublication' task.")
            }
          }

          dependsOn 'generatePomFileForMavenJavaPublication'
          into("META-INF/maven/${project.group}/${project.name}") { from "${pomFile}" }

          dependsOn project.generatePomPropertiesFileForMavenJavaPublication
          into("META-INF/maven/${project.group}/${project.name}") { from "${pomPropertiesFile}" }
        }

        // Only build artifacts for archives if we are publishing
        project.artifacts.archives project.shadowJar
        project.artifacts.archives project.shadowTestJar

        project.task('sourcesJar', type: Jar) {
          from project.sourceSets.main.allSource
          classifier = 'sources'
        }
        project.artifacts.archives project.sourcesJar

        project.task('testSourcesJar', type: Jar) {
          from project.sourceSets.test.allSource
          classifier = 'test-sources'
        }
        project.artifacts.archives project.testSourcesJar

        project.task('javadocJar', type: Jar, dependsOn: project.javadoc) {
          classifier = 'javadoc'
          from project.javadoc.destinationDir
        }
        project.artifacts.archives project.javadocJar

        project.publishing {
          repositories {
            maven {
              name "testPublicationLocal"
              url "file://${project.rootProject.projectDir}/testPublication/"
            }
            maven {
              url(project.properties['distMgmtSnapshotsUrl'] ?: isRelease(project)
                      ? 'https://repository.apache.org/service/local/staging/deploy/maven2'
                      : 'https://repository.apache.org/content/repositories/snapshots')

              // We attempt to find and load credentials from ~/.m2/settings.xml file that a user
              // has configured with the Apache release and snapshot staging credentials.
              // <settings>
              //   <servers>
              //     <server>
              //       <id>apache.releases.https</id>
              //       <username>USER_TOKEN</username>
              //       <password>PASS_TOKEN</password>
              //     </server>
              //     <server>
              //       <id>apache.snapshots.https</id>
              //       <username>USER_TOKEN</username>
              //       <password>PASS_TOKEN</password>
              //     </server>
              //   </servers>
              // </settings>
              def settingsXml = new File(System.getProperty('user.home'), '.m2/settings.xml')
              if (settingsXml.exists()) {
                def serverId = (project.properties['distMgmtServerId'] ?: isRelease(project)
                        ? 'apache.releases.https' : 'apache.snapshots.https')
                def m2SettingCreds = new XmlSlurper().parse(settingsXml).servers.server.find { server -> serverId.equals(server.id.text()) }
                if (m2SettingCreds) {
                  credentials {
                    username m2SettingCreds.username.text()
                    password m2SettingCreds.password.text()
                  }
                }
              }
            }
          }

          publications {
            mavenJava(MavenPublication) {
              artifact project.shadowJar
              artifact project.shadowTestJar
              artifact project.sourcesJar
              artifact project.testSourcesJar
              artifact project.javadocJar

              pom {
                name = project.description
                if (project.hasProperty("summary")) {
                  description = project.summary
                }
                url = "http://beam.apache.org"
                inceptionYear = "2016"
                licenses {
                  license {
                    name = "Apache License, Version 2.0"
                    url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    distribution = "repo"
                  }
                }
                scm {
                  connection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  developerConnection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  url = "https://gitbox.apache.org/repos/asf?p=beam.git;a=summary"
                }
                issueManagement {
                  system = "jira"
                  url = "https://issues.apache.org/jira/browse/BEAM"
                }
                mailingLists {
                  mailingList {
                    name = "Beam Dev"
                    subscribe = "dev-subscribe@beam.apache.org"
                    unsubscribe = "dev-unsubscribe@beam.apache.org"
                    post = "dev@beam.apache.org"
                    archive = "http://www.mail-archive.com/dev%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam User"
                    subscribe = "user-subscribe@beam.apache.org"
                    unsubscribe = "user-unsubscribe@beam.apache.org"
                    post = "user@beam.apache.org"
                    archive = "http://www.mail-archive.com/user%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam Commits"
                    subscribe = "commits-subscribe@beam.apache.org"
                    unsubscribe = "commits-unsubscribe@beam.apache.org"
                    post = "commits@beam.apache.org"
                    archive = "http://www.mail-archive.com/commits%beam.apache.org"
                  }
                }
                developers {
                  developer {
                    name = "The Apache Beam Team"
                    email = "dev@beam.apache.org"
                    url = "http://beam.apache.org"
                    organization = "Apache Software Foundation"
                    organizationUrl = "http://www.apache.org"
                  }
                }
              }

              pom.withXml {
                def root = asNode()
                def dependenciesNode = root.appendNode('dependencies')
                def generateDependenciesFromConfiguration = { param ->
                  project.configurations."${param.configuration}".allDependencies.each {
                    def dependencyNode = dependenciesNode.appendNode('dependency')
                    dependencyNode.appendNode('groupId', it.group)
                    dependencyNode.appendNode('artifactId', it.name)
                    dependencyNode.appendNode('version', it.version)

                    dependencyNode.appendNode('scope', param.scope)

                    // Start with any exclusions that were added via configuration exclude rules.
                    // Then add all the exclusions that are specific to the dependency (if any
                    // were declared). Finally build the node that represents all exclusions.
                    def exclusions = []
                    exclusions += project.configurations."${param.configuration}".excludeRules
                    if (it.hasProperty('excludeRules')) {
                      exclusions += it.excludeRules
                    }
                    if (!exclusions.empty) {
                      def exclusionsNode = dependencyNode.appendNode('exclusions')
                      exclusions.each { exclude ->
                        def exclusionNode = exclusionsNode.appendNode('exclusion')
                        exclusionNode.appendNode('groupId', exclude.group)
                        exclusionNode.appendNode('artifactId', exclude.module)
                      }
                    }
                  }
                }

                // TODO: Should we use the runtime scope instead of the compile scope
                // which forces all our consumers to declare what they consume?
                generateDependenciesFromConfiguration(configuration: 'shadow', scope: 'compile')
                generateDependenciesFromConfiguration(configuration: 'provided', scope: 'provided')

                // NB: This must come after asNode() logic, as it seems asNode()
                // removes XML comments.
                // TODO: Load this from file?
                def elem = asElement()
                def hdr = elem.getOwnerDocument().createComment(
                        '''
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
''')
                elem.insertBefore(hdr, elem.getFirstChild())
              }
            }
          }
        }
        // Only sign artifacts if we are performing a release
        if (isRelease(project) && !project.hasProperty('noSigning')) {
          project.apply plugin: "signing"
          project.signing {
            useGpgCmd()
            sign project.publishing.publications
          }
        }
      }

      // Ban these dependencies from all configurations
      project.configurations.all {
        // guava-jdk5 brings in classes which conflict with guava
        exclude group: "com.google.guava", module: "guava-jdk5"
        // Ban the usage of the JDK tools as a library as this is system dependent
        exclude group: "jdk.tools", module: "jdk.tools"
        // protobuf-lite duplicates classes which conflict with protobuf-java
        exclude group: "com.google.protobuf", module: "protobuf-lite"
        // Exclude these test dependencies because they bundle other common
        // test libraries classes causing version conflicts. Users should rely
        // on using the yyy-core package instead of the yyy-all package.
        exclude group: "org.hamcrest", module: "hamcrest-all"
        exclude group: "org.mockito", module: "mockito-all"
      }

      // Force usage of the libraries defined within our common set found in the root
      // build.gradle instead of using Gradles default dependency resolution mechanism
      // which chooses the latest version available.
      //
      // TODO: Figure out whether we should force all dependency conflict resolution
      // to occur in the "shadow" and "shadowTest" configurations.
      project.configurations.all { config ->
        // The "errorprone" configuration controls the classpath used by errorprone static analysis, which
        // has different dependencies than our project.
        if (config.getName() != "errorprone") {
          config.resolutionStrategy {
            force project.library.java.values()
          }
        }
      }

      // These directories for when build actions are delegated to Gradle
      def gradleAptGeneratedMain = "${project.buildDir}/generated/source/apt/main"
      def gradleAptGeneratedTest = "${project.buildDir}/generated/source/apt/test"

      // These directories for when build actions are executed by Idea
      // IntelliJ does not add these source roots (that it owns!) unless hinted
      def ideaRoot = "${project.projectDir}/out"
      def ideaAptGeneratedMain = "${ideaRoot}/production/classes/generated"
      def ideaAptGeneratedTest = "${ideaRoot}/test/classes/generated_test"

      project.idea {
        module {
          sourceDirs += project.file(gradleAptGeneratedMain)
          testSourceDirs += project.file(gradleAptGeneratedTest)

          sourceDirs += project.file(ideaAptGeneratedMain)
          testSourceDirs += project.file(ideaAptGeneratedTest)

          generatedSourceDirs += [
            project.file(gradleAptGeneratedMain),
            project.file(gradleAptGeneratedTest),
            project.file(ideaAptGeneratedMain),
            project.file(ideaAptGeneratedTest)
          ]

        }
      }
    }

    // When applied in a module's build.gradle file, this closure provides task for running
    // IO integration tests (manually, without PerfKitBenchmarker).
    project.ext.enableJavaPerformanceTesting = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      // Task for running integration tests
      project.task('integrationTest', type: Test) {

        // Disable Gradle cache (it should not be used because the IT's won't run).
        outputs.upToDateWhen { false }

        include "**/*IT.class"
        systemProperties.beamTestPipelineOptions = configuration.integrationTestPipelineOptions
      }
    }

    // When applied in a module's build.gradle file, this closure adds task providing
    // additional dependencies that might be needed while running integration tests.
    project.ext.provideIntegrationTestingDependencies = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      project.dependencies {
        def runner = configuration.runner
        def filesystem = configuration.filesystem

        /* include dependencies required by runners */
        //if (runner?.contains('dataflow')) {
        if (runner?.equalsIgnoreCase('dataflow')) {
          testCompile it.project(path: ":beam-runners-google-cloud-dataflow-java", configuration: 'shadowTest')
        }

        if (runner?.equalsIgnoreCase('direct')) {
          testCompile it.project(path: ":beam-runners-direct-java", configuration: 'shadowTest')
        }

        if (runner?.equalsIgnoreCase('flink')) {
          testCompile it.project(path: ":beam-runners-flink_2.11", configuration: 'shadowTest')
        }

        /* include dependencies required by filesystems */
        if (filesystem?.equalsIgnoreCase('hdfs')) {
          testCompile it.project(path: ":beam-sdks-java-io-hadoop-file-system", configuration: 'shadowTest')
          shadowTest project.library.java.hadoop_client
        }

        /* include dependencies required by AWS S3 */
        if (filesystem?.equalsIgnoreCase('s3')) {
          testCompile it.project.project(path: ":beam-sdks-java-io-amazon-web-services", configuration: 'shadowTest')
        }
      }

      project.task('packageIntegrationTests', type: Jar)
    }

    // When applied in a module's build gradle file, this closure provides a task
    // that will involve PerfKitBenchmarker for running integrationTests.
    project.ext.createPerformanceTestHarness = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      // This task runs PerfKitBenchmarker, which does benchmarking of the IO ITs.
      // The arguments passed to it allows it to invoke gradle again with the desired benchmark.
      //
      // To invoke this, run:
      //
      // ./gradlew performanceTest \
      //  -DpkbLocation="<path to pkb.py>"
      //  -DintegrationTestPipelineOptions='["--numberOfRecords=1000", "<more options>"]' \
      //  -DintegrationTest=<io test, eg. org.apache.beam.sdk.io.text.TextIOIT> \
      //  -DitModule=<directory containing desired test, eg. sdks/java/io/file-based-io-tests> \
      //  -DintegrationTestRunner=<runner to be used for testing, eg. dataflow>
      //
      // There are more options with default values that can be tweaked if needed (see below).
      project.task('performanceTest', type: Exec) {

        // PerfKitBenchmarker needs to work in the Beam's root directory,
        // otherwise it requires absolute paths ./gradlew, kubernetes scripts etc.
        commandLine "${configuration.pkbLocation}",
                "--dpb_log_level=${configuration.logLevel}",
                "--gradle_binary=${configuration.gradleBinary}",
                "--official=${configuration.isOfficial}",
                "--benchmarks=${configuration.benchmarks}",
                "--beam_location=${project.rootProject.projectDir}",

                "--beam_prebuilt=${configuration.beamPrebuilt}",
                "--beam_sdk=${configuration.beamSdk}",

                "--beam_it_timeout=${configuration.timeout}",

                "--kubeconfig=${configuration.kubeconfig}",
                "--kubectl=${configuration.kubectl}",
                "--beam_kubernetes_scripts=${configuration.kubernetesScripts}",

                "--beam_it_options=${configuration.integrationTestPipelineOptions}",
                "--beam_options_config_file=${configuration.optionsConfigFile}",

                "--beam_it_class=${configuration.integrationTest}",
                "--beam_it_module=${configuration.itModule}",

                "--beam_extra_properties=${configuration.extraProperties}",
                "--beam_runner=${configuration.runner}"
      }
    }

    /** ***********************************************************************************************/

    project.ext.applyGoNature = {
      project.apply plugin: "com.github.blindpirate.gogradle"
      project.golang { goVersion = '1.10' }

      project.repositories {
        golang {
          // Gogradle doesn't like thrift: https://github.com/gogradle/gogradle/issues/183
          root 'git.apache.org/thrift.git'
          emptyDir()
        }
        golang {
          root 'github.com/apache/thrift'
          emptyDir()
        }
      }

      project.idea {
        module {
          // The gogradle plugin downloads all dependencies into the source tree here,
          // which is a path baked into golang
          excludeDirs += project.file("${project.path}/vendor")

          // gogradle's private working directory
          excludeDirs += project.file("${project.path}/.gogradle")
        }
      }
    }

    /** ***********************************************************************************************/

    project.ext.applyDockerNature = {
      project.apply plugin: "com.palantir.docker"
      project.docker { noCache true }
    }

    // containerImageName returns a configurable container image name, by default a
    // development image at bintray.io (see sdks/CONTAINERS.md):
    //
    //     $USER-docker-apache.bintray.io/beam/$NAME:latest
    //
    // Both the root and tag can be defined using properties or explicitly provided.
    project.ext.containerImageName = {
      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      ContainerImageNameConfiguration configuration = it ? it as ContainerImageNameConfiguration : new ContainerImageNameConfiguration()

      if (configuration.root == null) {
        if (project.rootProject.hasProperty(["docker-repository-root"])) {
          configuration.root = project.rootProject["docker-repository-root"]
        } else {
          configuration.root = "${System.properties["user.name"]}-docker-apache.bintray.io/beam"
        }
      }
      if (configuration.tag == null) {
        if (project.rootProject.hasProperty(["docker-tag"])) {
          configuration.tag = project.rootProject["docker-tag"]
        } else {
          configuration.tag = 'latest'
        }
      }
      return "${configuration.root}/${configuration.name}:${configuration.tag}"
    }

    /** ***********************************************************************************************/

    project.ext.applyGrpcNature = {
      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc { // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:3.6.0" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:1.13.1" }
        }

        generateProtoTasks {
          ofSourceSet("main")*.plugins {
            // Apply the "grpc" plugin whose spec is defined above, without
            // options.  Note the braces cannot be omitted, otherwise the
            // plugin will not be added. This is because of the implicit way
            // NamedDomainObjectContainer binds the methods.
            grpc {}
          }
        }
      }

      def generatedProtoMainJavaDir = "${project.buildDir}/generated/source/proto/main/java"
      def generatedProtoTestJavaDir = "${project.buildDir}/generated/source/proto/test/java"
      def generatedGrpcMainJavaDir = "${project.buildDir}/generated/source/proto/main/grpc"
      def generatedGrpcTestJavaDir = "${project.buildDir}/generated/source/proto/test/grpc"
      project.idea {
        module {
          sourceDirs += project.file(generatedProtoMainJavaDir)
          generatedSourceDirs += project.file(generatedProtoMainJavaDir)

          testSourceDirs += project.file(generatedProtoTestJavaDir)
          generatedSourceDirs += project.file(generatedProtoTestJavaDir)

          sourceDirs += project.file(generatedGrpcMainJavaDir)
          generatedSourceDirs += project.file(generatedGrpcMainJavaDir)

          testSourceDirs += project.file(generatedGrpcTestJavaDir)
          generatedSourceDirs += project.file(generatedGrpcTestJavaDir)
        }
      }
    }

    /** ***********************************************************************************************/

    project.ext.applyPortabilityNature = {
      println "applyPortabilityNature with " + (it ? "$it" : "default configuration") + " for project $project.name"
      project.ext.applyJavaNature(enableFindbugs: false, shadowClosure: {
        // The relocation paths below specifically use the major version number of the dependency
        // since we assume that packages following semantic versioning rules. For packages
        // with a major version number that is 0 (implying that the dependency is unstable),
        // we use the full version identifier in the relocated path (e.g. v0_minor_patch).
        // For stable dependencies, if two or more packages use the same major version number,
        // we should bias to vendoring the highest version to minimize jar files instead of
        // vendoring using their minor or patch version numbers. Some packages may be incompatible
        // across minor or patch versions and in those instances we should either attempt to update our
        // usage of the dependency so we don't need to vendor both or choose to vendor
        // both specifying the minor version in the relocation path.

        // To produce the list of necessary relocations, one needs to start with a set of target
        // packages that one wants to vendor, find all necessary transitive dependencies of that
        // set and provide relocations for each such that all necessary packages and their
        // dependencies are relocated. Any optional dependency that doesn't need relocation
        // must be excluded via an 'exclude' rule. There is additional complexity of libraries that use
        // JNI or reflection and have to be handled on case by case basis by learning whether
        // they support relocation and how would one go about doing it by reading any documentation
        // those libraries may provide. The 'validateShadedJarDoesntLeakNonOrgApacheBeamClasses'
        // ensures that there are no classes outside of the 'org.apache.beam' namespace.

        // guava uses the com.google.common and com.google.thirdparty package namespaces
        relocate "com.google.common", "org.apache.beam.vendor.guava.v20.com.google.common"
        relocate "com.google.thirdparty", "org.apache.beam.vendor.guava.v20.com.google.thirdparty"

        relocate "com.google.protobuf", "org.apache.beam.vendor.protobuf.v3.com.google.protobuf"
        relocate "com.google.gson", "org.apache.beam.vendor.gson.v2.com.google.gson"
        relocate "io.grpc", "org.apache.beam.vendor.grpc.v1.io.grpc"
        relocate "com.google.auth", "org.apache.beam.vendor.google_auth_library_credentials.v0_9_1.com.google.auth"
        relocate "com.google.api", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.api"
        relocate "com.google.cloud", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.cloud"
        relocate "com.google.logging", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.logging"
        relocate "com.google.longrunning", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.longrunning"
        relocate "com.google.rpc", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.rpc"
        relocate "com.google.type", "org.apache.beam.vendor.proto_google_common_protos.v1.com.google.type"
        relocate "io.opencensus", "org.apache.beam.vendor.opencensus.v0_12_3.io.opencensus"

        // Adapted from https://github.com/grpc/grpc-java/blob/e283f70ad91f99c7fee8b31b605ef12a4f9b1690/netty/shaded/build.gradle#L41
        relocate "io.netty", "org.apache.beam.vendor.netty.v4.io.netty"
        // We have to be careful with these replacements as they must not match any
        // string in NativeLibraryLoader, else they cause corruption. Note that
        // this includes concatenation of string literals and constants.
        relocate 'META-INF/native/libnetty', 'META-INF/native/liborg_apache_beam_vendor_netty_v4_netty'
        relocate 'META-INF/native/netty', 'META-INF/native/org_apache_beam_vendor_netty_v4_netty'

        // Don't include errorprone, JDK8 annotations, objenesis, junit, and mockito in the bundled jar
        exclude "com/google/errorprone/**"
        exclude "com/google/instrumentation/**"
        exclude "javax/annotation/**"
        exclude "junit/**"
        exclude "org/hamcrest/**"
        exclude "org/junit/**"
        exclude "org/mockito/**"
        exclude "org/objenesis/**"
      })

      // Don't force modules here because we don't want to take the shared declarations in build_rules.gradle
      // because we would like to have the freedom to choose which versions of dependencies we
      // are using for the portability APIs separate from what is being used inside other modules such as GCP.
      project.configurations.all { config ->
        config.resolutionStrategy { forcedModules = []}
      }

      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc { // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:3.6.0" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:1.13.1" }
        }

        generateProtoTasks {
          ofSourceSet("main")*.plugins {
            // Apply the "grpc" plugin whose spec is defined above, without
            // options.  Note the braces cannot be omitted, otherwise the
            // plugin will not be added. This is because of the implicit way
            // NamedDomainObjectContainer binds the methods.
            grpc { }
          }
        }
      }

      project.dependencies {
        compile 'com.google.guava:guava:20.0'
        compile 'com.google.protobuf:protobuf-java:3.6.0'
        compile 'com.google.protobuf:protobuf-java-util:3.6.0'
        compile 'com.google.code.gson:gson:2.7'
        compile 'io.grpc:grpc-auth:1.13.1'
        compile 'io.grpc:grpc-core:1.13.1'
        compile 'io.grpc:grpc-context:1.13.1'
        compile 'io.grpc:grpc-netty:1.13.1'
        compile 'io.grpc:grpc-protobuf:1.13.1'
        compile 'io.grpc:grpc-stub:1.13.1'
        compile 'io.netty:netty-transport-native-epoll:4.1.25.Final'
        compile 'io.netty:netty-tcnative-boringssl-static:2.0.8.Final'
        compile 'com.google.auth:google-auth-library-credentials:0.10.0'
        compile 'io.grpc:grpc-testing:1.13.1'
        compile 'com.google.api.grpc:proto-google-common-protos:1.12.0'
        compile 'io.opencensus:opencensus-api:0.12.3'
        compile 'io.opencensus:opencensus-contrib-grpc-metrics:0.12.3'
        shadow 'com.google.errorprone:error_prone_annotations:2.1.2'
      }

      // TODO(BEAM-4544): Integrate intellij support into this.
    }

    /** ***********************************************************************************************/

    // TODO: Decide whether this should be inlined into the one project that relies on it
    // or be left here.
    project.ext.applyAvroNature = { project.apply plugin: "com.commercehub.gradle.plugin.avro" }

    // Creates a task to run the quickstart for a runner.
    // Releases version and URL, can be overriden for a RC release with
    // ./gradlew :release:runJavaExamplesValidationTask -Pver=2.3.0 -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-1027
    project.ext.createJavaExamplesArchetypeValidationTask = {
      JavaExamplesArchetypeValidationConfiguration config = it as JavaExamplesArchetypeValidationConfiguration
      def taskName = "run${config.type}Java${config.runner}"
      def releaseVersion = project.findProperty('ver') ?: project.version
      def releaseRepo = project.findProperty('repourl') ?: 'https://repository.apache.org/content/repositories/snapshots'
      def argsNeeded = [
        "--ver=${releaseVersion}",
        "--repourl=${releaseRepo}"
      ]
      if (config.gcpProject) {
        argsNeeded.add("--gcpProject=${config.gcpProject}")
      }
      if (config.gcsBucket) {
        argsNeeded.add("--gcsBucket=${config.gcsBucket}")
      }
      if (config.bqDataset) {
        argsNeeded.add("--bqDataset=${config.bqDataset}")
      }
      if (config.pubsubTopic) {
        argsNeeded.add("--pubsubTopic=${config.pubsubTopic}")
      }
      project.evaluationDependsOn(':release')
      project.task(taskName, dependsOn: ':release:classes', type: JavaExec) {
        group = "Verification"
        description = "Run the Beam ${config.type} with the ${config.runner} runner"
        main = "${config.type}-java-${config.runner}".toLowerCase()
        classpath = project.project(':release').sourceSets.main.runtimeClasspath
        args argsNeeded
      }
    }


    /** ***********************************************************************************************/

    // Method to create the PortableValidatesRunnerTask.
    // The method takes PortableValidatesRunnerConfiguration as parameter.
    project.ext.createPortableValidatesRunnerTask = {
      /*
       * We need to rely on manually specifying these evaluationDependsOn to ensure that
       * the following projects are evaluated before we evaluate this project. This is because
       * we are attempting to reference the "sourceSets.test.output" directly.
       */
      project.evaluationDependsOn(":beam-sdks-java-core")
      project.evaluationDependsOn(":beam-runners-core-java")
      def config = it ? it as PortableValidatesRunnerConfiguration : new PortableValidatesRunnerConfiguration()
      def name = config.name
      def beamTestPipelineOptions = [
        "--runner=org.apache.beam.runners.reference.testing.TestPortableRunner",
        "--jobServerDriver=${config.jobServerDriver}",
      ]
      if (config.jobServerConfig) {
        beamTestPipelineOptions.add("--jobServerConfig=${config.jobServerConfig}")
      }
      project.tasks.create(name: name, type: Test) {
        group = "Verification"
        description = "Validates the PortableRunner with JobServer ${config.jobServerDriver}"
        systemProperty "beamTestPipelineOptions", JsonOutput.toJson(beamTestPipelineOptions)
        classpath = config.testClasspathConfiguration
        testClassesDirs = project.files(project.project(":beam-sdks-java-core").sourceSets.test.output.classesDirs, project.project(":beam-runners-core-java").sourceSets.test.output.classesDirs)
        maxParallelForks 1
        useJUnit(config.testCategories)
      }
    }
  }
}
