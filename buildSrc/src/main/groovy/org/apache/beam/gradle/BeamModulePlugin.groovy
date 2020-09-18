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
import groovy.json.JsonSlurper
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.file.FileCollection
import org.gradle.api.file.FileTree
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.testing.jacoco.tasks.JacocoReport

import java.util.concurrent.atomic.AtomicInteger
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

  /** Licence header enforced by spotless */
  static final String javaLicenseHeader = """/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""
  static AtomicInteger startingExpansionPortNumber = new AtomicInteger(18091)

  /** A class defining the set of configurable properties accepted by applyJavaNature. */
  class JavaNatureConfiguration {
    /** Controls whether the spotbugs plugin is enabled and configured. */
    boolean enableSpotbugs = true

    /** Controls whether the checker framework plugin is enabled and configured. */
    boolean enableChecker = true

    /** Controls whether legacy rawtype usage is allowed. */
    boolean ignoreRawtypeErrors = false

    /** Controls whether the dependency analysis plugin is enabled. */
    boolean enableStrictDependencies = false

    /** Override the default "beam-" + `dash separated path` archivesBaseName. */
    String archivesBaseName = null;

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
     * The set of excludes that should be used during validation of the shadow jar. Projects should override
     * the default with the most specific set of excludes that is valid for the contents of its shaded jar.
     *
     * By default we exclude any class underneath the org.apache.beam namespace.
     */
    List<String> shadowJarValidationExcludes = ["org/apache/beam/**"]

    /**
     * If unset, no shading is performed. The jar and test jar archives are used during publishing.
     * Otherwise the shadowJar and shadowTestJar artifacts are used during publishing.
     *
     * The shadowJar / shadowTestJar tasks execute the specified closure to configure themselves.
     */
    Closure shadowClosure;

    /** Controls whether this project is published to Maven. */
    boolean publish = true

    /** Controls whether javadoc is exported for this project. */
    boolean exportJavadoc = true

    /**
     * Automatic-Module-Name Header value to be set in MANFIEST.MF file.
     * This is a required parameter unless publishing to Maven is disabled for this project.
     *
     * @see: https://github.com/GoogleCloudPlatform/cloud-opensource-java/blob/master/library-best-practices/JLBP-20.md
     */
    String automaticModuleName = null

    /**
     * The set of additional maven repositories that should be added into published POM file.
     */
    List<Map> mavenRepositories = [];
  }

  /** A class defining the set of configurable properties accepted by applyPortabilityNature. */
  class PortabilityNatureConfiguration {
    /**
     * The set of excludes that should be used during validation of the shadow jar. Projects should override
     * the default with the most specific set of excludes that is valid for the contents of its shaded jar.
     *
     * By default we exclude any class underneath the org.apache.beam namespace.
     */
    List<String> shadowJarValidationExcludes = ["org/apache/beam/**"]

    /** Override the default "beam-" + `dash separated path` archivesBaseName. */
    String archivesBaseName = null;

    /** Controls whether this project is published to Maven. */
    boolean publish = true

    /**
     * Automatic-Module-Name Header value to be set in MANFIEST.MF file.
     * This is a required parameter unless publishing to Maven is disabled for this project.
     *
     * @see: https://github.com/GoogleCloudPlatform/cloud-opensource-java/blob/master/library-best-practices/JLBP-20.md
     */
    String automaticModuleName
  }

  // A class defining the set of configurable properties for createJavaExamplesArchetypeValidationTask
  class JavaExamplesArchetypeValidationConfiguration {
    // Type [Quickstart, MobileGaming] for the postrelease validation is required.
    // Used both for the test name run${type}Java${runner}
    // and also for the script name, ${type}-java-${runner}.toLowerCase().
    String type

    // runner [Direct, Dataflow, Spark, Flink, FlinkLocal]
    String runner

    // gcpProject sets the gcpProject argument when executing examples.
    String gcpProject

    // gcpRegion sets the region for executing Dataflow examples.
    String gcpRegion

    // gcsBucket sets the gcsProject argument when executing examples.
    String gcsBucket

    // bqDataset sets the BigQuery Dataset when executing mobile-gaming examples
    String bqDataset

    // pubsubTopic sets topics when executing streaming pipelines
    String pubsubTopic
  }

  // Reads and contains all necessary performance test parameters
  class JavaPerformanceTestConfiguration {
    // Optional. Runner which will be used for running the tests. Possible values: dataflow/direct.
    // PerfKitBenchmarker will have trouble reading 'null' value. It expects empty string if no config file is expected.
    String runner = System.getProperty('integrationTestRunner', '')

    // Optional. Filesystem which will be used for running the tests. Possible values: hdfs.
    // if not specified runner's local filesystem will be used.
    String filesystem = System.getProperty('filesystem')

    // Required. Pipeline options to be used by the tested pipeline.
    String integrationTestPipelineOptions = System.getProperty('integrationTestPipelineOptions')
  }

  // Reads and contains all necessary performance test parameters
  class PythonPerformanceTestConfiguration {
    // Fully qualified name of the test to run.
    String tests = System.getProperty('tests')

    // Attribute tag that can filter the test set.
    String attribute = System.getProperty('attr')

    // Extra test options pass to nose.
    String[] extraTestOptions = ["--nocapture"]

    // Name of Cloud KMS encryption key to use in some tests.
    String kmsKeyName = System.getProperty('kmsKeyName')

    // Pipeline options to be used for pipeline invocation.
    String pipelineOptions = System.getProperty('pipelineOptions', '')
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
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // Extra options to pass to TestPipeline
    String[] pipelineOpts = []
    // Spin up the Harness inside a DOCKER container
    Environment environment = Environment.DOCKER
    // Categories for tests to run.
    Closure testCategories = {
      includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      // Use the following to include / exclude categories:
      // includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      // excludeCategories 'org.apache.beam.sdk.testing.FlattenWithHeterogeneousCoders'
    }
    // Tests to include/exclude from running, by default all tests are included
    Closure testFilter = {
      // Use the following to include / exclude tests:
      // includeTestsMatching 'org.apache.beam.sdk.transforms.FlattenTest.testFlattenWithDifferentInputAndOutputCoders2'
      // excludeTestsMatching 'org.apache.beam.sdk.transforms.FlattenTest.testFlattenWithDifferentInputAndOutputCoders2'
    }
    // Configuration for the classpath when running the test.
    Configuration testClasspathConfiguration
    // Additional system properties.
    Properties systemProperties = []

    enum Environment {
      DOCKER,   // Docker-based Harness execution
      PROCESS,  // Process-based Harness execution
      EMBEDDED, // Execute directly inside the execution engine (testing only)
    }
  }

  // A class defining the configuration for CrossLanguageValidatesRunner.
  class CrossLanguageValidatesRunnerConfiguration {
    // Task name for cross-language validate runner case.
    String name = 'validatesCrossLanguageRunner'
    // Job endpoint to use.
    String jobEndpoint = 'localhost:8099'
    // Job server startup task.
    Task startJobServer
    // Job server cleanup task.
    Task cleanupJobServer
    // Number of parallel test runs.
    Integer numParallelTests = 1
    // Extra options to pass to TestPipeline
    String[] pipelineOpts = []
    // Categories for tests to run.
    Closure testCategories = {
      includeCategories 'org.apache.beam.sdk.testing.UsesCrossLanguageTransforms'
      // Use the following to include / exclude categories:
      // includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
      // excludeCategories 'org.apache.beam.sdk.testing.FlattenWithHeterogeneousCoders'
    }
    // classpath for running tests.
    FileCollection classpath
  }

  def isRelease(Project project) {
    return project.hasProperty('isRelease')
  }

  def defaultArchivesBaseName(Project p) {
    return 'beam' + p.path.replace(':', '-')
  }

  void apply(Project project) {

    /** ***********************************************************************************************/
    // Apply common properties/repositories and tasks to all projects.

    project.ext.mavenGroupId = 'org.apache.beam'

    // Automatically use the official release version if we are performing a release
    // otherwise append '-SNAPSHOT'
    project.version = '2.25.0'
    if (!isRelease(project)) {
      project.version += '-SNAPSHOT'
    }

    // Default to dash-separated directories for artifact base name,
    // which will also be the default artifactId for maven publications
    project.apply plugin: 'base'
    project.archivesBaseName = defaultArchivesBaseName(project)

    project.apply plugin: 'org.apache.beam.jenkins'

    // Register all Beam repositories and configuration tweaks
    Repositories.register(project)

    // Apply a plugin which enables configuring projects imported into Intellij.
    project.apply plugin: "idea"

    // Provide code coverage
    // TODO: Should this only apply to Java projects?
    project.apply plugin: "jacoco"
    project.gradle.taskGraph.whenReady { graph ->
      // Disable jacoco unless report requested such that task outputs can be properly cached.
      // https://discuss.gradle.org/t/do-not-cache-if-condition-matched-jacoco-agent-configured-with-append-true-satisfied/23504
      def enabled = graph.allTasks.any { it instanceof JacocoReport || it.name.contains("javaPreCommit") }
      project.tasks.withType(Test) { jacoco.enabled = enabled }
    }

    // Apply a plugin which provides tasks for dependency / property / task reports.
    // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
    // for further details. This is typically very useful to look at the "htmlDependencyReport"
    // when attempting to resolve dependency issues.
    project.apply plugin: "project-report"

    // Apply a plugin which fails the build if there is a dependency on a transitive
    // non-declared dependency, since these can break users (as in BEAM-6558)
    //
    // Though this is Java-specific, it is required to be applied to the root
    // project due to implementation-details of the plugin. It can be enabled/disabled
    // via JavaNatureConfiguration per project. It is disabled by default until we can
    // make all of our deps good.
    project.apply plugin: "ca.cutterslade.analyze"

    // Adds a taskTree task that prints task dependency tree report to the console.
    // Useful for investigating build issues.
    // See: https://github.com/dorongold/gradle-task-tree
    project.apply plugin: "com.dorongold.task-tree"
    project.taskTree { noRepeat = true }

    /** ***********************************************************************************************/
    // Define and export a map dependencies shared across multiple sub-projects.
    //
    // Example usage:
    // configuration {
    //   compile library.java.avro
    //   testCompile library.java.junit
    // }

    // These versions are defined here because they represent
    // a dependency version which should match across multiple
    // Maven artifacts.
    def aws_java_sdk_version = "1.11.718"
    def aws_java_sdk2_version = "2.13.54"
    def cassandra_driver_version = "3.8.0"
    def checkerframework_version = "3.5.0"
    def classgraph_version = "4.8.65"
    def gax_version = "1.57.1"
    def generated_grpc_ga_version = "1.85.1"
    def google_auth_version = "0.19.0"
    def google_clients_version = "1.30.10"
    def google_cloud_bigdataoss_version = "2.1.3"
    def google_cloud_core_version = "1.93.7"
    def google_cloud_pubsublite_version = "0.1.6"
    def google_cloud_spanner_version = "1.59.0"
    def google_cloud_datacatalog_version = "0.32.1"
    def google_code_gson_version = "2.8.6"
    def google_http_clients_version = "1.34.0"
    def google_oauth_clients_version = "1.30.6"
    def grpc_version = "1.27.2"
    def guava_version = "25.1-jre"
    def hadoop_version = "2.8.5"
    def hamcrest_version = "2.1"
    def jackson_version = "2.10.2"
    def jaxb_api_version = "2.3.3"
    def kafka_version = "1.0.0"
    def nemo_version = "0.1"
    def netty_version = "4.1.51.Final"
    def postgres_version = "42.2.2"
    def powermock_version = "2.0.2"
    def proto_google_common_protos_version = "1.17.0"
    def protobuf_version = "3.11.1"
    def quickcheck_version = "0.8"
    def slf4j_version = "1.7.30"
    def spark_version = "2.4.6"
    def spotbugs_version = "4.0.6"
    def testcontainers_kafka_version = "1.14.3"
    def testcontainers_localstack_version = "1.14.3"

    // A map of maps containing common libraries used per language. To use:
    // dependencies {
    //   compile library.java.slf4j_api
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
        antlr                                       : "org.antlr:antlr4:4.7",
        antlr_runtime                               : "org.antlr:antlr4-runtime:4.7",
        args4j                                      : "args4j:args4j:2.33",
        avro                                        : "org.apache.avro:avro:1.8.2",
        avro_tests                                  : "org.apache.avro:avro:1.8.2:tests",
        aws_java_sdk_cloudwatch                     : "com.amazonaws:aws-java-sdk-cloudwatch:$aws_java_sdk_version",
        aws_java_sdk_core                           : "com.amazonaws:aws-java-sdk-core:$aws_java_sdk_version",
        aws_java_sdk_dynamodb                       : "com.amazonaws:aws-java-sdk-dynamodb:$aws_java_sdk_version",
        aws_java_sdk_kinesis                        : "com.amazonaws:aws-java-sdk-kinesis:$aws_java_sdk_version",
        aws_java_sdk_s3                             : "com.amazonaws:aws-java-sdk-s3:$aws_java_sdk_version",
        aws_java_sdk_sns                            : "com.amazonaws:aws-java-sdk-sns:$aws_java_sdk_version",
        aws_java_sdk_sqs                            : "com.amazonaws:aws-java-sdk-sqs:$aws_java_sdk_version",
        aws_java_sdk_sts                            : "com.amazonaws:aws-java-sdk-sts:$aws_java_sdk_version",
        aws_java_sdk2_apache_client                 : "software.amazon.awssdk:apache-client:$aws_java_sdk2_version",
        aws_java_sdk2_auth                          : "software.amazon.awssdk:auth:$aws_java_sdk2_version",
        aws_java_sdk2_cloudwatch                    : "software.amazon.awssdk:cloudwatch:$aws_java_sdk2_version",
        aws_java_sdk2_dynamodb                      : "software.amazon.awssdk:dynamodb:$aws_java_sdk2_version",
        aws_java_sdk2_kinesis                       : "software.amazon.awssdk:kinesis:$aws_java_sdk2_version",
        aws_java_sdk2_sdk_core                      : "software.amazon.awssdk:sdk-core:$aws_java_sdk2_version",
        aws_java_sdk2_sns                           : "software.amazon.awssdk:sns:$aws_java_sdk2_version",
        aws_java_sdk2_sqs                           : "software.amazon.awssdk:sqs:$aws_java_sdk2_version",
        bigdataoss_gcsio                            : "com.google.cloud.bigdataoss:gcsio:$google_cloud_bigdataoss_version",
        bigdataoss_util                             : "com.google.cloud.bigdataoss:util:$google_cloud_bigdataoss_version",
        cassandra_driver_core                       : "com.datastax.cassandra:cassandra-driver-core:$cassandra_driver_version",
        cassandra_driver_mapping                    : "com.datastax.cassandra:cassandra-driver-mapping:$cassandra_driver_version",
        classgraph                                  : "io.github.classgraph:classgraph:$classgraph_version",
        commons_codec                               : "commons-codec:commons-codec:1.14",
        commons_compress                            : "org.apache.commons:commons-compress:1.20",
        commons_csv                                 : "org.apache.commons:commons-csv:1.8",
        commons_io                                  : "commons-io:commons-io:2.6",
        commons_lang3                               : "org.apache.commons:commons-lang3:3.9",
        commons_math3                               : "org.apache.commons:commons-math3:3.6.1",
        error_prone_annotations                     : "com.google.errorprone:error_prone_annotations:2.3.1",
        gax                                         : "com.google.api:gax:$gax_version",
        gax_grpc                                    : "com.google.api:gax-grpc:$gax_version",
        google_api_client                           : "com.google.api-client:google-api-client:$google_clients_version",
        google_api_client_jackson2                  : "com.google.api-client:google-api-client-jackson2:$google_clients_version",
        google_api_client_java6                     : "com.google.api-client:google-api-client-java6:$google_clients_version",
        google_api_common                           : "com.google.api:api-common:1.8.1",
        google_api_services_bigquery                : "com.google.apis:google-api-services-bigquery:v2-rev20200719-$google_clients_version",
        google_api_services_clouddebugger           : "com.google.apis:google-api-services-clouddebugger:v2-rev20200501-$google_clients_version",
        google_api_services_cloudresourcemanager    : "com.google.apis:google-api-services-cloudresourcemanager:v1-rev20200720-$google_clients_version",
        google_api_services_dataflow                : "com.google.apis:google-api-services-dataflow:v1b3-rev20200713-$google_clients_version",
        google_api_services_healthcare              : "com.google.apis:google-api-services-healthcare:v1beta1-rev20200713-$google_clients_version",
        google_api_services_pubsub                  : "com.google.apis:google-api-services-pubsub:v1-rev20200713-$google_clients_version",
        google_api_services_storage                 : "com.google.apis:google-api-services-storage:v1-rev20200611-$google_clients_version",
        google_auth_library_credentials             : "com.google.auth:google-auth-library-credentials:$google_auth_version",
        google_auth_library_oauth2_http             : "com.google.auth:google-auth-library-oauth2-http:$google_auth_version",
        google_cloud_bigquery                       : "com.google.cloud:google-cloud-bigquery:1.108.0",
        google_cloud_bigquery_storage               : "com.google.cloud:google-cloud-bigquerystorage:0.125.0-beta",
        google_cloud_bigtable_client_core           : "com.google.cloud.bigtable:bigtable-client-core:1.16.0",
        google_cloud_core                           : "com.google.cloud:google-cloud-core:$google_cloud_core_version",
        google_cloud_core_grpc                      : "com.google.cloud:google-cloud-core-grpc:$google_cloud_core_version",
        google_cloud_datacatalog_v1beta1            : "com.google.cloud:google-cloud-datacatalog:$google_cloud_datacatalog_version",
        google_cloud_dataflow_java_proto_library_all: "com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:0.5.160304",
        google_cloud_datastore_v1_proto_client      : "com.google.cloud.datastore:datastore-v1-proto-client:1.6.3",
        google_cloud_pubsublite                     : "com.google.cloud:google-cloud-pubsublite:$google_cloud_pubsublite_version",
        google_cloud_spanner                        : "com.google.cloud:google-cloud-spanner:$google_cloud_spanner_version",
        google_code_gson                            : "com.google.code.gson:gson:$google_code_gson_version",
        google_http_client                          : "com.google.http-client:google-http-client:$google_http_clients_version",
        google_http_client_apache_v2                : "com.google.http-client:google-http-client-apache-v2:$google_http_clients_version",
        google_http_client_jackson                  : "com.google.http-client:google-http-client-jackson:1.29.2",
        google_http_client_jackson2                 : "com.google.http-client:google-http-client-jackson2:$google_http_clients_version",
        google_http_client_protobuf                 : "com.google.http-client:google-http-client-protobuf:$google_http_clients_version",
        google_oauth_client                         : "com.google.oauth-client:google-oauth-client:$google_oauth_clients_version",
        google_oauth_client_java6                   : "com.google.oauth-client:google-oauth-client-java6:$google_oauth_clients_version",
        // Don't use grpc_all, it can cause issues in Bazel builds. Reference the gRPC libraries you need individually instead.
        grpc_api                                    : "io.grpc:grpc-api:$grpc_version",
        grpc_alts                                   : "io.grpc:grpc-alts:$grpc_version",
        grpc_auth                                   : "io.grpc:grpc-auth:$grpc_version",
        grpc_core                                   : "io.grpc:grpc-core:$grpc_version",
        grpc_context                                : "io.grpc:grpc-context:$grpc_version",
        grpc_google_cloud_pubsub_v1                 : "com.google.api.grpc:grpc-google-cloud-pubsub-v1:$generated_grpc_ga_version",
        grpc_google_cloud_pubsublite_v1             : "com.google.api.grpc:grpc-google-cloud-pubsublite-v1:$google_cloud_pubsublite_version",
        grpc_grpclb                                 : "io.grpc:grpc-grpclb:$grpc_version",
        grpc_protobuf                               : "io.grpc:grpc-protobuf:$grpc_version",
        grpc_protobuf_lite                          : "io.grpc:grpc-protobuf-lite:$grpc_version",
        grpc_netty                                  : "io.grpc:grpc-netty:$grpc_version",
        grpc_netty_shaded                           : "io.grpc:grpc-netty-shaded:$grpc_version",
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
        jackson_jaxb_annotations                    : "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:$jackson_version",
        jackson_core                                : "com.fasterxml.jackson.core:jackson-core:$jackson_version",
        jackson_databind                            : "com.fasterxml.jackson.core:jackson-databind:$jackson_version",
        jackson_dataformat_cbor                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:$jackson_version",
        jackson_dataformat_csv                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:$jackson_version",
        jackson_dataformat_xml                      : "com.fasterxml.jackson.dataformat:jackson-dataformat-xml:$jackson_version",
        jackson_dataformat_yaml                     : "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jackson_version",
        jackson_datatype_joda                       : "com.fasterxml.jackson.datatype:jackson-datatype-joda:$jackson_version",
        jackson_module_scala                        : "com.fasterxml.jackson.module:jackson-module-scala_2.11:$jackson_version",
        jaxb_api                                    : "jakarta.xml.bind:jakarta.xml.bind-api:$jaxb_api_version",
        jaxb_impl                                   : "com.sun.xml.bind:jaxb-impl:$jaxb_api_version",
        joda_time                                   : "joda-time:joda-time:2.10.5",
        jsonassert                                  : "org.skyscreamer:jsonassert:1.5.0",
        jsr305                                      : "com.google.code.findbugs:jsr305:3.0.2",
        junit                                       : "junit:junit:4.13-beta-3",
        kafka                                       : "org.apache.kafka:kafka_2.11:$kafka_version",
        kafka_clients                               : "org.apache.kafka:kafka-clients:$kafka_version",
        mockito_core                                : "org.mockito:mockito-core:3.0.0",
        nemo_compiler_frontend_beam                 : "org.apache.nemo:nemo-compiler-frontend-beam:$nemo_version",
        netty_handler                               : "io.netty:netty-handler:$netty_version",
        netty_tcnative_boringssl_static             : "io.netty:netty-tcnative-boringssl-static:2.0.33.Final",
        netty_transport_native_epoll                : "io.netty:netty-transport-native-epoll:$netty_version",
        postgres                                    : "org.postgresql:postgresql:$postgres_version",
        powermock                                   : "org.powermock:powermock-module-junit4:$powermock_version",
        powermock_mockito                           : "org.powermock:powermock-api-mockito2:$powermock_version",
        protobuf_java                               : "com.google.protobuf:protobuf-java:$protobuf_version",
        protobuf_java_util                          : "com.google.protobuf:protobuf-java-util:$protobuf_version",
        proto_google_cloud_bigquery_storage_v1beta1 : "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1beta1:0.85.1",
        proto_google_cloud_bigtable_v2              : "com.google.api.grpc:proto-google-cloud-bigtable-v2:1.9.1",
        proto_google_cloud_datastore_v1             : "com.google.api.grpc:proto-google-cloud-datastore-v1:0.85.0",
        proto_google_cloud_pubsub_v1                : "com.google.api.grpc:proto-google-cloud-pubsub-v1:$generated_grpc_ga_version",
        proto_google_cloud_pubsublite_v1            : "com.google.api.grpc:proto-google-cloud-pubsublite-v1:$google_cloud_pubsublite_version",
        proto_google_cloud_spanner_admin_database_v1: "com.google.api.grpc:proto-google-cloud-spanner-admin-database-v1:$google_cloud_spanner_version",
        proto_google_common_protos                  : "com.google.api.grpc:proto-google-common-protos:$proto_google_common_protos_version",
        slf4j_api                                   : "org.slf4j:slf4j-api:$slf4j_version",
        slf4j_simple                                : "org.slf4j:slf4j-simple:$slf4j_version",
        slf4j_jdk14                                 : "org.slf4j:slf4j-jdk14:$slf4j_version",
        slf4j_log4j12                               : "org.slf4j:slf4j-log4j12:$slf4j_version",
        snappy_java                                 : "org.xerial.snappy:snappy-java:1.1.4",
        spark_core                                  : "org.apache.spark:spark-core_2.11:$spark_version",
        spark_network_common                        : "org.apache.spark:spark-network-common_2.11:$spark_version",
        spark_sql                                   : "org.apache.spark:spark-sql_2.11:$spark_version",
        spark_streaming                             : "org.apache.spark:spark-streaming_2.11:$spark_version",
        stax2_api                                   : "org.codehaus.woodstox:stax2-api:3.1.4",
        testcontainers_localstack                   : "org.testcontainers:localstack:$testcontainers_localstack_version",
        testcontainers_kafka                        : "org.testcontainers:kafka:$testcontainers_kafka_version",
        vendored_bytebuddy_1_10_8                   : "org.apache.beam:beam-vendor-bytebuddy-1_10_8:0.1",
        vendored_grpc_1_26_0                        : "org.apache.beam:beam-vendor-grpc-1_26_0:0.3",
        vendored_guava_26_0_jre                     : "org.apache.beam:beam-vendor-guava-26_0-jre:0.1",
        vendored_calcite_1_20_0                     : "org.apache.beam:beam-vendor-calcite-1_20_0:0.1",
        woodstox_core_asl                           : "org.codehaus.woodstox:woodstox-core-asl:4.4.1",
        zstd_jni                                    : "com.github.luben:zstd-jni:1.4.5-2",
        quickcheck_core                             : "com.pholser:junit-quickcheck-core:$quickcheck_version",
      ],
      groovy: [
        groovy_all: "org.codehaus.groovy:groovy-all:2.4.13",
      ],
      // For generating pom.xml from archetypes.
      maven: [
        maven_compiler_plugin: "maven-plugins:maven-compiler-plugin:3.7.0",
        maven_exec_plugin    : "maven-plugins:maven-exec-plugin:1.6.0",
        maven_jar_plugin     : "maven-plugins:maven-jar-plugin:3.0.2",
        maven_shade_plugin   : "maven-plugins:maven-shade-plugin:3.1.0",
        maven_surefire_plugin: "maven-plugins:maven-surefire-plugin:3.0.0-M5",
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

    project.ext.repositories = {
      maven {
        name "testPublicationLocal"
        url "file://${project.rootProject.projectDir}/testPublication/"
      }
      maven {
        url(project.properties['distMgmtSnapshotsUrl'] ?: isRelease(project)
            ? 'https://repository.apache.org/service/local/staging/deploy/maven2'
            : 'https://repository.apache.org/content/repositories/snapshots')
        name(project.properties['distMgmtServerId'] ?: isRelease(project)
            ? 'apache.releases.https' : 'apache.snapshots.https')
        // The maven settings plugin will load credentials from ~/.m2/settings.xml file that a user
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
      }
    }

    // Configures a project with a default set of plugins that should apply to all Java projects.
    //
    // Users should invoke this method using Groovy map syntax. For example:
    // applyJavaNature(enableSpotbugs: true)
    //
    // See JavaNatureConfiguration for the set of accepted properties.
    //
    // The following plugins are enabled:
    //  * java
    //  * maven
    //  * net.ltgt.apt (plugin to configure annotation processing tool)
    //  * propdeps (provide optional and provided dependency configurations)
    //  * propdeps-maven
    //  * propdeps-idea
    //  * checkstyle
    //  * spotbugs
    //  * shadow (conditional on shadowClosure being specified)
    //  * com.diffplug.spotless (code style plugin)
    //
    // Dependency Management for Java Projects
    // ---------------------------------------
    //
    // By default, the shadow plugin is not enabled. It is only enabled by specifying a shadowClosure
    // as an argument. If no shadowClosure has been specified, dependencies should fall into the
    // configurations as described within the Gradle documentation (https://docs.gradle.org/current/userguide/java_plugin.html#sec:java_plugin_and_dependency_management)
    //
    // When the shadowClosure argument is specified, the shadow plugin is enabled to perform shading
    // of commonly found dependencies. Because of this it is important that dependencies are added
    // to the correct configuration. Dependencies should fall into one of these four configurations:
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
    // When creating a cross-project dependency between two Java projects, one should only rely on
    // the shaded configurations if the project has a shadowClosure being specified. This allows
    // for compilation/test execution to occur against the final artifact that will be provided to
    // users. This is by done by referencing the "shadow" or "shadowTest" configuration as so:
    //   dependencies {
    //     shadow project(path: "other:java:project1", configuration: "shadow")
    //     shadowTest project(path: "other:java:project2", configuration: "shadowTest")
    //   }
    // This will ensure the correct set of transitive dependencies from those projects are correctly
    // added to the main and test source set runtimes.

    project.ext.applyJavaNature = {
      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      JavaNatureConfiguration configuration = it ? it as JavaNatureConfiguration : new JavaNatureConfiguration()

      if (configuration.archivesBaseName) {
        project.archivesBaseName = configuration.archivesBaseName
      }

      project.apply plugin: "java"

      // Configure the Java compiler source language and target compatibility levels. Also ensure that
      // we configure the Java compiler to use UTF-8.
      project.sourceCompatibility = project.javaVersion
      project.targetCompatibility = project.javaVersion

      def defaultLintSuppressions = [
        'options',
        'cast',
        // https://bugs.openjdk.java.net/browse/JDK-8190452
        'classfile',
        'deprecation',
        'fallthrough',
        'processing',
        'serial',
        'try',
        'unchecked',
        'varargs',
      ]

      if (configuration.ignoreRawtypeErrors) {
        defaultLintSuppressions.add("rawtypes")
      }

      project.tasks.withType(JavaCompile) {
        options.encoding = "UTF-8"
        // As we want to add '-Xlint:-deprecation' we intentionally remove '-Xlint:deprecation' from compilerArgs here,
        // as intellij is adding this, see https://youtrack.jetbrains.com/issue/IDEA-196615
        options.compilerArgs -= ["-Xlint:deprecation"]
        options.compilerArgs += ([
          '-parameters',
          '-Xlint:all',
          '-Werror'
        ]
        + (defaultLintSuppressions + configuration.disableLintWarnings).collect { "-Xlint:-${it}" })
      }

      if (project.hasProperty("compileAndRunTestsWithJava11")) {
        def java11Home = project.findProperty("java11Home")
        project.tasks.compileTestJava {
          options.fork = true
          options.forkOptions.javaHome = java11Home as File
          options.compilerArgs += ['-Xlint:-path']
          options.compilerArgs.addAll(['--release', '11'])
        }
        project.tasks.withType(Test) {
          useJUnit()
          executable = "${java11Home}/bin/java"
        }
      }

      // Configure the default test tasks set of tests executed
      // to match the equivalent set that is executed by the maven-surefire-plugin.
      // See http://maven.apache.org/components/surefire/maven-surefire-plugin/test-mojo.html
      project.test {
        include "**/Test*.class"
        include "**/*Test.class"
        include "**/*Tests.class"
        include "**/*TestCase.class"
        // fixes issues with test filtering on multi-module project
        // see https://discuss.gradle.org/t/multi-module-build-fails-with-tests-filter/25835
        filter { setFailOnNoMatchingTests(false) }
      }

      project.tasks.withType(Test) {
        // Configure all test tasks to use JUnit
        useJUnit {}
        // default maxHeapSize on gradle 5 is 512m, lets increase to handle more demanding tests
        maxHeapSize = '2g'
      }

      // Most of our modules have null errors. Once they are fixed, we can
      // set enableChecker=true in the build.gradle. Until then, we can pass -PenableChecker to
      // find a few errors and fix them.
      if (configuration.enableChecker) {
        project.apply plugin: 'org.checkerframework'

        project.checkerFramework {
          checkers = [
            'org.checkerframework.checker.nullness.NullnessChecker'
          ]
          extraJavacArgs = [
            '-AskipDefs=AutoValue_.*'
          ]
        }

        project.dependencies {
          checkerFramework("org.checkerframework:checker:$checkerframework_version")
        }
      }

      if (configuration.shadowClosure) {
        // Ensure that tests are packaged and part of the artifact set.
        project.task('packageTests', type: Jar) {
          classifier = 'tests-unshaded'
          from project.sourceSets.test.output
        }
        project.artifacts.archives project.packageTests
      }

      // Configures annotation processing for commonly used annotation processors
      // across all Java projects.
      project.apply plugin: "net.ltgt.apt"
      // let idea apt plugin handle the ide integration
      project.apply plugin: "net.ltgt.apt-idea"

      // Note that these plugins specifically use the compileOnly and testCompileOnly
      // configurations because they are never required to be shaded or become a
      // dependency of the output.
      def compileOnlyAnnotationDeps = [
        "com.google.auto.value:auto-value-annotations:1.7",
        "com.google.auto.service:auto-service-annotations:1.0-rc6",
        "com.google.j2objc:j2objc-annotations:1.3",
        // This contains many improved annotations beyond javax.annotations for enhanced static checking
        // of the codebase
        "org.checkerframework:checker-qual:$checkerframework_version",
        // These dependencies are needed to avoid error-prone warnings on package-info.java files,
        // also to include the annotations to suppress warnings.
        //
        // spotbugs-annotations artifact is licensed under LGPL and cannot be included in the
        // Apache Beam distribution, but may be relied on during build.
        // See: https://www.apache.org/legal/resolved.html#prohibited
        // Special case for jsr305 (a transitive dependency of spotbugs-annotations):
        // sdks/java/core's FieldValueTypeInformation needs javax.annotations.Nullable at runtime.
        // Therefore, the java core module declares jsr305 dependency (BSD license) as "compile".
        // https://github.com/findbugsproject/findbugs/blob/master/findbugs/licenses/LICENSE-jsr305.txt
        "com.github.spotbugs:spotbugs-annotations:$spotbugs_version",
        "net.jcip:jcip-annotations:1.0",
        // This explicitly adds javax.annotation.Generated (SOURCE retention)
        // as a compile time dependency since Java 9+ no longer includes common
        // EE annotations: http://bugs.openjdk.java.net/browse/JDK-8152842. This
        // is required for grpc: http://github.com/grpc/grpc-java/issues/5343.
        //
        // javax.annotation is licensed under GPL 2.0 with Classpath Exception
        // and must not be included in the Apache Beam distribution, but may be
        // relied on during build.
        // See exception in: https://www.apache.org/legal/resolved.html#prohibited
        // License: https://github.com/javaee/javax.annotation/blob/1.3.2/LICENSE
        "javax.annotation:javax.annotation-api:1.3.2",
      ]

      project.dependencies {
        compileOnlyAnnotationDeps.each { dep ->
          compileOnly dep
          testCompileOnly dep
          annotationProcessor dep
          testAnnotationProcessor dep
        }

        // Add common annotation processors to all Java projects
        def annotationProcessorDeps = [
          "com.google.auto.value:auto-value:1.7",
          "com.google.auto.service:auto-service:1.0-rc6",
        ]

        annotationProcessorDeps.each { dep ->
          annotationProcessor dep
          testAnnotationProcessor dep
        }
      }

      // Add the optional and provided configurations for dependencies
      // TODO: Either remove these plugins and find another way to generate the Maven poms
      // with the correct dependency scopes configured.
      project.apply plugin: 'propdeps'
      project.apply plugin: 'propdeps-maven'
      project.apply plugin: 'propdeps-idea'

      // Defines Targets for sonarqube analysis reporting.
      project.apply plugin: "org.sonarqube"

      // Configures a checkstyle plugin enforcing a set of rules and also allows for a set of
      // suppressions.
      project.apply plugin: 'checkstyle'
      project.tasks.withType(Checkstyle) {
        configFile = project.project(":").file("sdks/java/build-tools/src/main/resources/beam/checkstyle.xml")
        configProperties = ["checkstyle.suppressions.file": project.project(":").file("sdks/java/build-tools/src/main/resources/beam/suppressions.xml")]
        showViolations = true
        maxErrors = 0
      }
      project.checkstyle { toolVersion = "8.23" }

      // Configures javadoc plugin and ensure check runs javadoc.
      project.tasks.withType(Javadoc) {
        options.encoding = 'UTF-8'
        options.addBooleanOption('Xdoclint:-missing', true)
      }
      project.check.dependsOn project.javadoc

      // Apply the eclipse and apt-eclipse plugins.  This adds the "eclipse" task and
      // connects the apt-eclipse plugin to update the eclipse project files
      // with the instructions needed to run apt within eclipse to handle the AutoValue
      // and additional annotations
      project.apply plugin: 'eclipse'
      project.apply plugin: "net.ltgt.apt-eclipse"

      // Enables a plugin which can apply code formatting to source.
      project.apply plugin: "com.diffplug.spotless"
      // scan CVE
      project.apply plugin: "net.ossindex.audit"
      project.audit { rateLimitAsError = false }
      // Spotless can be removed from the 'check' task by passing -PdisableSpotlessCheck=true on the Gradle
      // command-line. This is useful for pre-commit which runs spotless separately.
      def disableSpotlessCheck = project.hasProperty('disableSpotlessCheck') &&
          project.disableSpotlessCheck == 'true'
      project.spotless {
        enforceCheck !disableSpotlessCheck
        java {
          licenseHeader javaLicenseHeader
          googleJavaFormat('1.7')
          target project.fileTree(project.projectDir) { include 'src/*/java/**/*.java' }
        }
      }

      // Enables a plugin which performs code analysis for common bugs.
      // This plugin is configured to only analyze the "main" source set.
      if (configuration.enableSpotbugs) {
        project.tasks.whenTaskAdded {task ->
          if(task.name.contains("spotbugsTest")) {
            task.enabled = false
          }
        }
        project.apply plugin: 'com.github.spotbugs'
        project.dependencies {
          spotbugs "com.github.spotbugs:spotbugs:$spotbugs_version"
          spotbugs "com.google.auto.value:auto-value:1.7"
          compileOnlyAnnotationDeps.each { dep -> spotbugs dep }
        }
        project.spotbugs {
          excludeFilter = project.rootProject.file('sdks/java/build-tools/src/main/resources/beam/spotbugs-filter.xml')
        }
        project.tasks.withType(com.github.spotbugs.snom.SpotBugsTask) {
          reports {
            html.enabled = !project.jenkins.isCIBuild
            xml.enabled = project.jenkins.isCIBuild
          }
        }
      }

      // Disregard unused but declared (test) compile only dependencies used
      // for common annotation classes used during compilation such as annotation
      // processing or post validation such as spotbugs.
      project.dependencies {
        compileOnlyAnnotationDeps.each { dep ->
          permitUnusedDeclared dep
          permitTestUnusedDeclared dep
        }
      }
      if (configuration.enableStrictDependencies) {
        project.tasks.analyzeClassesDependencies.enabled = true
        project.tasks.analyzeDependencies.enabled = true
        project.tasks.analyzeTestClassesDependencies.enabled = false
      } else {
        project.tasks.analyzeClassesDependencies.enabled = false
        project.tasks.analyzeTestClassesDependencies.enabled = false
        project.tasks.analyzeDependencies.enabled = false
      }

      // Enable errorprone static analysis
      project.apply plugin: 'net.ltgt.errorprone'

      project.dependencies {
        errorprone("com.google.errorprone:error_prone_core:2.3.1")
        // At least JDk 9 compiler is required, however JDK 8 still can be used but with additional errorproneJavac
        // configuration. For more details please see https://github.com/tbroyer/gradle-errorprone-plugin#jdk-8-support
        errorproneJavac("com.google.errorprone:javac:9+181-r4173-1")
      }

      project.configurations.errorprone { resolutionStrategy.force 'com.google.errorprone:error_prone_core:2.3.1' }

      project.tasks.withType(JavaCompile) {
        options.errorprone.disableWarningsInGeneratedCode = true
        options.errorprone.excludedPaths = '(.*/)?(build/generated-src|build/generated.*avro-java|build/generated)/.*'
        options.errorprone.errorproneArgs.add("MutableConstantField:OFF")
      }

      if (configuration.shadowClosure) {
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
      }

      project.jar {
        setAutomaticModuleNameHeader(configuration, project)

        zip64 true
        into("META-INF/") {
          from "${project.rootProject.projectDir}/LICENSE"
          from "${project.rootProject.projectDir}/NOTICE"
        }
      }

      // Always configure the shadowJar classifier and merge service files.
      if (configuration.shadowClosure) {
        // Only set the classifer on the unshaded classes if we are shading.
        project.jar { classifier = "unshaded" }

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
          project.task('validateShadedJarDoesntLeakNonProjectClasses', dependsOn: 'shadowJar') {
            ext.outFile = project.file("${project.reportsDir}/${name}.out")
            inputs.files project.configurations.shadow.artifacts.files
            outputs.files outFile
            doLast {
              project.configurations.shadow.artifacts.files.each {
                FileTree exposedClasses = project.zipTree(it).matching {
                  include "**/*.class"
                  // BEAM-5919: Exclude paths for Java 9 multi-release jars.
                  exclude "META-INF/versions/*/module-info.class"
                  configuration.shadowJarValidationExcludes.each {
                    exclude "$it"
                    exclude "META-INF/versions/*/$it"
                  }
                }
                outFile.text = exposedClasses.files
                if (exposedClasses.files) {
                  throw new GradleException("$it exposed classes outside of ${configuration.shadowJarValidationExcludes}: ${exposedClasses.files}")
                }
              }
            }
          }
          project.tasks.check.dependsOn project.tasks.validateShadedJarDoesntLeakNonProjectClasses
        }
      } else {
        project.task("testJar", type: Jar, {
          group = "Jar"
          description = "Create a JAR of test classes"
          classifier = "tests"
          from project.sourceSets.test.output
          zip64 true
          exclude "META-INF/INDEX.LIST"
          exclude "META-INF/*.SF"
          exclude "META-INF/*.DSA"
          exclude "META-INF/*.RSA"
        })
        project.artifacts.testRuntime project.testJar
      }

      project.ext.includeInJavaBom = configuration.publish
      project.ext.exportJavadoc = configuration.exportJavadoc

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
                       groupId=${project.mavenGroupId}
                       artifactId=${project.archivesBaseName}"""
          }
        }

        // Have the main artifact jar include both the generate pom.xml and its properties file
        // emulating the behavior of the maven-archiver plugin.
        project.(configuration.shadowClosure ? 'shadowJar' : 'jar') {
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
          into("META-INF/maven/${project.mavenGroupId}/${project.archivesBaseName}") {
            from "${pomFile}"
            rename('.*', 'pom.xml')
          }

          dependsOn project.generatePomPropertiesFileForMavenJavaPublication
          into("META-INF/maven/${project.mavenGroupId}/${project.archivesBaseName}") { from "${pomPropertiesFile}" }
        }

        // Only build artifacts for archives if we are publishing
        if (configuration.shadowClosure) {
          project.artifacts.archives project.shadowJar
          project.artifacts.archives project.shadowTestJar
        } else {
          project.artifacts.archives project.jar
          project.artifacts.archives project.testJar
        }

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
          repositories project.ext.repositories

          publications {
            mavenJava(MavenPublication) {
              if (configuration.shadowClosure) {
                artifact project.shadowJar
                artifact project.shadowTestJar
              } else {
                artifact project.jar
                artifact project.testJar
              }
              artifact project.sourcesJar
              artifact project.testSourcesJar
              artifact project.javadocJar

              artifactId = project.archivesBaseName
              groupId = project.mavenGroupId

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

                // Add "repositories" section if it was configured
                if (configuration.mavenRepositories && configuration.mavenRepositories.size() > 0) {
                  def repositoriesNode = root.appendNode('repositories')
                  configuration.mavenRepositories.each {
                    def repositoryNode = repositoriesNode.appendNode('repository')
                    repositoryNode.appendNode('id', it.get('id'))
                    repositoryNode.appendNode('url', it.get('url'))
                  }
                }

                def dependenciesNode = root.appendNode('dependencies')
                def generateDependenciesFromConfiguration = { param ->
                  project.configurations."${param.configuration}".allDependencies.each {
                    def dependencyNode = dependenciesNode.appendNode('dependency')
                    def appendClassifier = { dep ->
                      dep.artifacts.each { art ->
                        if (art.hasProperty('classifier')) {
                          dependencyNode.appendNode('classifier', art.classifier)
                        }
                      }
                    }

                    if (it instanceof ProjectDependency) {
                      dependencyNode.appendNode('groupId', it.getDependencyProject().mavenGroupId)
                      dependencyNode.appendNode('artifactId', it.getDependencyProject().archivesBaseName)
                      dependencyNode.appendNode('version', it.version)
                      dependencyNode.appendNode('scope', param.scope)
                      appendClassifier(it)
                    } else {
                      dependencyNode.appendNode('groupId', it.group)
                      dependencyNode.appendNode('artifactId', it.name)
                      dependencyNode.appendNode('version', it.version)
                      dependencyNode.appendNode('scope', param.scope)
                      appendClassifier(it)
                    }

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
                generateDependenciesFromConfiguration(
                    configuration: (configuration.shadowClosure ? 'shadow' : 'compile'), scope: 'compile')
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
      }

      // Force usage of the libraries defined within our common set found in the root
      // build.gradle instead of using Gradles default dependency resolution mechanism
      // which chooses the latest version available.
      //
      // TODO: Figure out whether we should force all dependency conflict resolution
      // to occur in the "shadow" and "shadowTest" configurations.
      project.configurations.all { config ->
        // When running beam_Dependency_Check, resolutionStrategy should not be used; otherwise
        // gradle-versions-plugin does not report the latest versions of the dependencies.
        def startTasks = project.gradle.startParameter.taskNames
        def inDependencyUpdates = 'dependencyUpdates' in startTasks || 'runBeamDependencyCheck' in startTasks

        // The "errorprone" configuration controls the classpath used by errorprone static analysis, which
        // has different dependencies than our project.
        if (config.getName() != "errorprone" && !inDependencyUpdates) {
          config.resolutionStrategy {
            force project.library.java.values()
          }
        }
      }
    }

    // When applied in a module's build.gradle file, this closure provides task for running
    // IO integration tests.
    project.ext.enableJavaPerformanceTesting = {

      // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
      // See: http://groovy-lang.org/closures.html#implicit-it
      JavaPerformanceTestConfiguration configuration = it ? it as JavaPerformanceTestConfiguration : new JavaPerformanceTestConfiguration()

      // Task for running integration tests
      project.task('integrationTest', type: Test) {

        // Disable Gradle cache (it should not be used because the IT's won't run).
        outputs.upToDateWhen { false }

        include "**/*IT.class"

        def pipelineOptionsString = configuration.integrationTestPipelineOptions
        def pipelineOptionsStringFormatted
        def allOptionsList

        if(pipelineOptionsString) {
          allOptionsList = (new JsonSlurper()).parseText(pipelineOptionsString)
        }

        if(pipelineOptionsString && configuration.runner?.equalsIgnoreCase('dataflow')) {
          project.evaluationDependsOn(":runners:google-cloud-dataflow-java:worker:legacy-worker")
          def dataflowWorkerJar = project.findProperty('dataflowWorkerJar') ?:
              project.project(":runners:google-cloud-dataflow-java:worker:legacy-worker").shadowJar.archivePath
          def dataflowRegion = project.findProperty('dataflowRegion') ?: 'us-central1'
          allOptionsList.addAll([
            '--workerHarnessContainerImage=',
            "--dataflowWorkerJar=${dataflowWorkerJar}",
            "--region=${dataflowRegion}"
          ])
        }

        // Windows handles quotation marks differently
        if (pipelineOptionsString && System.properties['os.name'].toLowerCase().contains('windows')) {
          def allOptionsListFormatted = allOptionsList.collect{ "\"$it\"" }
          pipelineOptionsStringFormatted = JsonOutput.toJson(allOptionsListFormatted)
        } else if (pipelineOptionsString) {
          pipelineOptionsStringFormatted = JsonOutput.toJson(allOptionsList)
        }

        systemProperties.beamTestPipelineOptions = pipelineOptionsStringFormatted ?: pipelineOptionsString
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
          testRuntime it.project(path: ":runners:google-cloud-dataflow-java", configuration: 'testRuntime')
          testRuntime it.project(path: ":runners:google-cloud-dataflow-java:worker:legacy-worker", configuration: 'shadow')
        }

        if (runner?.equalsIgnoreCase('direct')) {
          testRuntime it.project(path: ":runners:direct-java", configuration: 'shadowTest')
        }

        if (runner?.equalsIgnoreCase('flink')) {
          testRuntime it.project(path: ":runners:flink:1.10", configuration: 'testRuntime')
        }

        if (runner?.equalsIgnoreCase('spark')) {
          testRuntime it.project(path: ":runners:spark", configuration: 'testRuntime')
          testRuntime project.library.java.spark_core
          testRuntime project.library.java.spark_streaming

          // Testing the Spark runner causes a StackOverflowError if slf4j-jdk14 is on the classpath
          project.configurations.testRuntimeClasspath {
            exclude group: "org.slf4j", module: "slf4j-jdk14"
          }
        }

        /* include dependencies required by filesystems */
        if (filesystem?.equalsIgnoreCase('hdfs')) {
          testRuntime it.project(path: ":sdks:java:io:hadoop-file-system", configuration: 'testRuntime')
          testRuntime project.library.java.hadoop_client
        }

        /* include dependencies required by AWS S3 */
        if (filesystem?.equalsIgnoreCase('s3')) {
          testRuntime it.project(path: ":sdks:java:io:amazon-web-services", configuration: 'testRuntime')
        }
      }
      project.task('packageIntegrationTests', type: Jar)
    }

    /** ***********************************************************************************************/

    project.ext.applyGoNature = {
      // Define common lifecycle tasks and artifact types
      project.apply plugin: 'base'

      project.apply plugin: "com.github.blindpirate.gogradle"
      project.golang { goVersion = '1.12' }

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
        project.clean.dependsOn project.goClean
        project.check.dependsOn project.goCheck
        project.assemble.dependsOn project.goBuild
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
      project.tasks.create(name: "copyLicenses", type: Copy) {
        from "${project.rootProject.projectDir}/LICENSE"
        from "${project.rootProject.projectDir}/NOTICE"
        into "build/target"
      }
      project.tasks.dockerPrepare.dependsOn project.tasks.copyLicenses
    }

    project.ext.applyDockerRunNature = {
      project.apply plugin: "com.palantir.docker-run"
    }
    /** ***********************************************************************************************/

    project.ext.applyGroovyNature = {
      project.apply plugin: "groovy"

      project.apply plugin: "com.diffplug.spotless"
      def disableSpotlessCheck = project.hasProperty('disableSpotlessCheck') &&
          project.disableSpotlessCheck == 'true'
      project.spotless {
        enforceCheck !disableSpotlessCheck
        def grEclipseConfig = project.project(":").file("buildSrc/greclipse.properties")
        groovy {
          greclipse().configFile(grEclipseConfig)
          target project.fileTree(project.projectDir) { include '**/*.groovy' }
        }
        groovyGradle { greclipse().configFile(grEclipseConfig) }
      }
    }

    // containerImageName returns a configurable container image name, by default a
    // development image at docker.io (see sdks/CONTAINERS.md):
    //
    //    format: apache/beam_$NAME_sdk:latest
    //    ie: apache/beam_python3.7_sdk:latest apache/beam_java_sdk:latest apache/beam_go_sdk:latest
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

    // applyGrpcNature should only be applied to projects who wish to use
    // unvendored gRPC / protobuf dependencies.
    project.ext.applyGrpcNature = {
      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc {
          // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:$protobuf_version" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:$grpc_version" }
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

    // applyPortabilityNature should only be applied to projects that want to use
    // vendored gRPC / protobuf dependencies.
    project.ext.applyPortabilityNature = {
      PortabilityNatureConfiguration configuration = it ? it as PortabilityNatureConfiguration : new PortabilityNatureConfiguration()

      if (configuration.archivesBaseName) {
        project.archivesBaseName = configuration.archivesBaseName
      }

      project.ext.applyJavaNature(
          exportJavadoc: false,
          enableSpotbugs: false,
          enableChecker: false,
          publish: configuration.publish,
          archivesBaseName: configuration.archivesBaseName,
          automaticModuleName: configuration.automaticModuleName,
          shadowJarValidationExcludes: it.shadowJarValidationExcludes,
          shadowClosure: GrpcVendoring_1_26_0.shadowClosure() << {
            // We perform all the code relocations but don't include
            // any of the actual dependencies since they will be supplied
            // by org.apache.beam:beam-vendor-grpc-v1p26p0:0.1
            dependencies {
              include(dependency { return false })
            }
          })

      // Don't force modules here because we don't want to take the shared declarations in build_rules.gradle
      // because we would like to have the freedom to choose which versions of dependencies we
      // are using for the portability APIs separate from what is being used inside other modules such as GCP.
      project.configurations.all { config ->
        config.resolutionStrategy { forcedModules = []}
      }

      project.apply plugin: "com.google.protobuf"
      project.protobuf {
        protoc {
          // The artifact spec for the Protobuf Compiler
          artifact = "com.google.protobuf:protoc:${GrpcVendoring_1_26_0.protobuf_version}" }

        // Configure the codegen plugins
        plugins {
          // An artifact spec for a protoc plugin, with "grpc" as
          // the identifier, which can be referred to in the "plugins"
          // container of the "generateProtoTasks" closure.
          grpc { artifact = "io.grpc:protoc-gen-grpc-java:${GrpcVendoring_1_26_0.grpc_version}" }
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

      project.dependencies GrpcVendoring_1_26_0.dependenciesClosure() << { shadow project.ext.library.java.vendored_grpc_1_26_0 }
    }

    /** ***********************************************************************************************/

    // TODO: Decide whether this should be inlined into the one project that relies on it
    // or be left here.
    project.ext.applyAvroNature = { project.apply plugin: "com.commercehub.gradle.plugin.avro" }

    project.ext.applyAntlrNature = {
      project.apply plugin: 'antlr'
      project.idea {
        module {
          // mark antlrs output folders as generated
          generatedSourceDirs += project.generateGrammarSource.outputDirectory
          generatedSourceDirs += project.generateTestGrammarSource.outputDirectory
        }
      }
    }

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
      if (config.gcpRegion) {
        argsNeeded.add("--gcpRegion=${config.gcpRegion}")
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
      project.evaluationDependsOn(":sdks:java:core")
      project.evaluationDependsOn(":runners:core-java")
      def config = it ? it as PortableValidatesRunnerConfiguration : new PortableValidatesRunnerConfiguration()
      def name = config.name
      def beamTestPipelineOptions = [
        "--runner=org.apache.beam.runners.portability.testing.TestPortableRunner",
        "--jobServerDriver=${config.jobServerDriver}",
        "--environmentCacheMillis=10000",
        "--experiments=beam_fn_api",
      ]
      beamTestPipelineOptions.addAll(config.pipelineOpts)
      if (config.environment == PortableValidatesRunnerConfiguration.Environment.EMBEDDED) {
        beamTestPipelineOptions += "--defaultEnvironmentType=EMBEDDED"
      }
      if (config.jobServerConfig) {
        beamTestPipelineOptions.add("--jobServerConfig=${config.jobServerConfig}")
      }
      config.systemProperties.put("beamTestPipelineOptions", JsonOutput.toJson(beamTestPipelineOptions))
      project.tasks.create(name: name, type: Test) {
        group = "Verification"
        description = "Validates the PortableRunner with JobServer ${config.jobServerDriver}"
        systemProperties config.systemProperties
        classpath = config.testClasspathConfiguration
        testClassesDirs = project.files(project.project(":sdks:java:core").sourceSets.test.output.classesDirs, project.project(":runners:core-java").sourceSets.test.output.classesDirs)
        maxParallelForks config.numParallelTests
        useJUnit(config.testCategories)
        filter(config.testFilter)
        // increase maxHeapSize as this is directly correlated to direct memory,
        // see https://issues.apache.org/jira/browse/BEAM-6698
        maxHeapSize = '4g'
        if (config.environment == PortableValidatesRunnerConfiguration.Environment.DOCKER) {
          dependsOn ':sdks:java:container:docker'
        }
      }
    }

    /** ***********************************************************************************************/

    // Method to create the crossLanguageValidatesRunnerTask.
    // The method takes crossLanguageValidatesRunnerConfiguration as parameter.
    project.ext.createCrossLanguageValidatesRunnerTask = {
      def config = it ? it as CrossLanguageValidatesRunnerConfiguration : new CrossLanguageValidatesRunnerConfiguration()

      project.evaluationDependsOn(":sdks:python")
      project.evaluationDependsOn(":sdks:java:testing:expansion-service")
      project.evaluationDependsOn(":runners:core-construction-java")

      // Task for launching expansion services
      def envDir = project.project(":sdks:python").envdir
      def pythonDir = project.project(":sdks:python").projectDir
      def javaPort = startingExpansionPortNumber.getAndDecrement()
      def pythonPort = startingExpansionPortNumber.getAndDecrement()
      def expansionJar = project.project(':sdks:java:testing:expansion-service').buildTestExpansionServiceJar.archivePath
      def expansionServiceOpts = [
        "group_id": project.name,
        "java_expansion_service_jar": expansionJar,
        "java_port": javaPort,
        "python_virtualenv_dir": envDir,
        "python_expansion_service_module": "apache_beam.runners.portability.expansion_service_test",
        "python_port": pythonPort
      ]
      def serviceArgs = project.project(':sdks:python').mapToArgString(expansionServiceOpts)
      def pythonContainerSuffix = project.project(':sdks:python').pythonVersion == '2.7' ? '2' : project.project(':sdks:python').pythonVersion.replace('.', '')
      def setupTask = project.tasks.create(name: config.name+"Setup", type: Exec) {
        dependsOn ':sdks:java:container:docker'
        dependsOn ':sdks:python:container:py'+pythonContainerSuffix+':docker'
        dependsOn ':sdks:java:testing:expansion-service:buildTestExpansionServiceJar'
        dependsOn ":sdks:python:installGcpTest"
        // setup test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_expansion_services.sh stop --group_id ${project.name} && $pythonDir/scripts/run_expansion_services.sh start $serviceArgs"
      }

      def mainTask = project.tasks.create(name: config.name) {
        group = "Verification"
        description = "Validates cross-language capability of runner"
      }

      def cleanupTask = project.tasks.create(name: config.name+'Cleanup', type: Exec) {
        // teardown test env
        executable 'sh'
        args '-c', "$pythonDir/scripts/run_expansion_services.sh stop --group_id ${project.name}"
      }
      setupTask.finalizedBy cleanupTask
      config.startJobServer.finalizedBy config.cleanupJobServer

      // Task for running testcases in Java SDK
      def beamJavaTestPipelineOptions = [
        "--runner=PortableRunner",
        "--jobEndpoint=${config.jobEndpoint}",
        "--environmentCacheMillis=10000",
        "--experiments=beam_fn_api",
      ]
      beamJavaTestPipelineOptions.addAll(config.pipelineOpts)
      ['Java': javaPort, 'Python': pythonPort].each { sdk, port ->
        def javaTask = project.tasks.create(name: config.name+"JavaUsing"+sdk, type: Test) {
          group = "Verification"
          description = "Validates runner for cross-language capability of using ${sdk} transforms from Java SDK"
          systemProperty "beamTestPipelineOptions", JsonOutput.toJson(beamJavaTestPipelineOptions)
          systemProperty "expansionJar", expansionJar
          systemProperty "expansionPort", port
          classpath = config.classpath
          testClassesDirs = project.files(project.project(":runners:core-construction-java").sourceSets.test.output.classesDirs)
          maxParallelForks config.numParallelTests
          useJUnit(config.testCategories)
          // increase maxHeapSize as this is directly correlated to direct memory,
          // see https://issues.apache.org/jira/browse/BEAM-6698
          maxHeapSize = '4g'
          dependsOn setupTask
          dependsOn config.startJobServer
        }
        mainTask.dependsOn javaTask
        cleanupTask.mustRunAfter javaTask
        config.cleanupJobServer.mustRunAfter javaTask

        // Task for running testcases in Python SDK
        def testOpts = [
          "--attr=UsesCrossLanguageTransforms"
        ]
        def pipelineOpts = [
          "--runner=PortableRunner",
          "--environment_cache_millis=10000",
          "--job_endpoint=${config.jobEndpoint}"
        ]
        def beamPythonTestPipelineOptions = [
          "pipeline_opts": pipelineOpts,
          "test_opts": testOpts,
          "suite": "xlangValidateRunner"
        ]
        def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)
        def pythonTask = project.tasks.create(name: config.name+"PythonUsing"+sdk, type: Exec) {
          group = "Verification"
          description = "Validates runner for cross-language capability of using ${sdk} transforms from Python SDK"
          environment "EXPANSION_JAR", expansionJar
          environment "EXPANSION_PORT", port
          executable 'sh'
          args '-c', ". $envDir/bin/activate && cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
          dependsOn setupTask
          dependsOn config.startJobServer
        }
        mainTask.dependsOn pythonTask
        cleanupTask.mustRunAfter pythonTask
        config.cleanupJobServer.mustRunAfter pythonTask
      }
      // Task for running testcases in Python SDK
      def testOpts = [
        "--attr=UsesSqlExpansionService"
      ]
      def pipelineOpts = [
        "--runner=PortableRunner",
        "--environment_cache_millis=10000",
        "--job_endpoint=${config.jobEndpoint}"
      ]
      def beamPythonTestPipelineOptions = [
        "pipeline_opts": pipelineOpts,
        "test_opts": testOpts,
        "suite": "xlangSqlValidateRunner"
      ]
      def cmdArgs = project.project(':sdks:python').mapToArgString(beamPythonTestPipelineOptions)
      def pythonSqlTask = project.tasks.create(name: config.name+"PythonUsingSql", type: Exec) {
        group = "Verification"
        description = "Validates runner for cross-language capability of using Java's SqlTransform from Python SDK"
        executable 'sh'
        args '-c', ". $envDir/bin/activate && cd $pythonDir && ./scripts/run_integration_test.sh $cmdArgs"
        dependsOn config.startJobServer
        dependsOn ':sdks:java:container:docker'
        dependsOn ':sdks:python:container:py'+pythonContainerSuffix+':docker'
        dependsOn ':sdks:java:extensions:sql:expansion-service:shadowJar'
        dependsOn ":sdks:python:installGcpTest"
      }
      mainTask.dependsOn pythonSqlTask
      config.cleanupJobServer.mustRunAfter pythonSqlTask
    }

    /** ***********************************************************************************************/

    project.ext.applyPythonNature = {

      // Define common lifecycle tasks and artifact types
      project.apply plugin: "base"

      // For some reason base doesn't define a test task  so we define it below and make
      // check depend on it. This makes the Python project similar to the task layout like
      // Java projects, see https://docs.gradle.org/4.2.1/userguide/img/javaPluginTasks.png
      if (project.tasks.findByName('test') == null) {
        project.task('test') {}
      }
      project.check.dependsOn project.test

      project.evaluationDependsOn(":runners:google-cloud-dataflow-java:worker")

      // Due to Beam-4256, we need to limit the length of virtualenv path to make the
      // virtualenv activated properly. So instead of include project name in the path,
      // we use the hash value.
      project.ext.envdir = "${project.rootProject.buildDir}/gradleenv/${project.path.hashCode()}"
      def pythonRootDir = "${project.rootDir}/sdks/python"

      // Python interpreter version for virtualenv setup and test run. This value can be
      // set from commandline with -PpythonVersion, or in build script of certain project.
      // If none of them applied, version set here will be used as default value.
      project.ext.pythonVersion = project.hasProperty('pythonVersion') ?
          project.pythonVersion : '2.7'

      project.task('setupVirtualenv')  {
        doLast {
          def virtualenvCmd = [
            'virtualenv',
            "${project.ext.envdir}",
            "--python=python${project.ext.pythonVersion}",
          ]
          project.exec { commandLine virtualenvCmd }
          project.exec {
            executable 'sh'
            args '-c', ". ${project.ext.envdir}/bin/activate && pip install --retries 10 --upgrade tox==3.11.1 -r ${project.rootDir}/sdks/python/build-requirements.txt"
          }
        }
        // Gradle will delete outputs whenever it thinks they are stale. Putting a
        // specific binary here could make gradle delete it while pip will believe
        // the package is fully installed.
        outputs.dirs(project.ext.envdir)
      }

      project.ext.pythonSdkDeps = project.files(
          project.fileTree(
          dir: "${project.rootDir}",
          include: ['model/**', 'sdks/python/**'],
          // Exclude temporary directories and files that are generated
          // during build and test.
          exclude: [
            '**/build/**',
            '**/dist/**',
            '**/target/**',
            '**/*.pyc',
            'sdks/python/*.egg*/**',
            'sdks/python/test-suites/**',
          ])
          )
      def copiedSrcRoot = "${project.buildDir}/srcs"

      // Create new configuration distTarBall which represents Python source
      // distribution tarball generated by :sdks:python:sdist.
      project.configurations { distTarBall }

      project.task('installGcpTest')  {
        dependsOn 'setupVirtualenv'
        dependsOn ':sdks:python:sdist'
        doLast {
          def distTarBall = "${pythonRootDir}/build/apache-beam.tar.gz"
          project.exec {
            executable 'sh'
            args '-c', ". ${project.ext.envdir}/bin/activate && pip install --retries 10 ${distTarBall}[gcp,test,aws,azure]"
          }
        }
      }

      project.task('cleanPython') {
        doLast {
          def activate = "${project.ext.envdir}/bin/activate"
          project.exec {
            executable 'sh'
            args '-c', "if [ -e ${activate} ]; then " +
                ". ${activate} && cd ${pythonRootDir} && python setup.py clean; " +
                "fi"
          }
          project.delete project.buildDir     // Gradle build directory
          project.delete project.ext.envdir   // virtualenv directory
          project.delete "$project.projectDir/target"   // tox work directory
        }
      }
      project.clean.dependsOn project.cleanPython
      // Force this subproject's clean to run before the main :clean, to avoid
      // racing on deletes.
      project.rootProject.clean.dependsOn project.clean

      // Return a joined String from a Map that contains all commandline args of
      // IT test.
      project.ext.mapToArgString = { argMap ->
        def argList = []
        argMap.each { k, v ->
          if (v in List) {
            v = "\"${v.join(' ')}\""
          } else if (v in String && v.contains(' ')) {
            // We should use double quote around the arg value if it contains series
            // of flags joined with space. Otherwise, commandline parsing of the
            // shell script will be broken.
            v = "\"${v.replace('"', '')}\""
          }
          argList.add("--$k $v")
        }
        return argList.join(' ')
      }

      project.ext.toxTask = { name, tox_env ->
        project.tasks.create(name) {
          dependsOn 'setupVirtualenv'
          dependsOn ':sdks:python:sdist'

          doLast {
            // Python source directory is also tox execution workspace, We want
            // to isolate them per tox suite to avoid conflict when running
            // multiple tox suites in parallel.
            project.copy { from project.pythonSdkDeps; into copiedSrcRoot }

            def copiedPyRoot = "${copiedSrcRoot}/sdks/python"
            def distTarBall = "${pythonRootDir}/build/apache-beam.tar.gz"
            project.exec {
              executable 'sh'
              args '-c', ". ${project.ext.envdir}/bin/activate && cd ${copiedPyRoot} && scripts/run_tox.sh $tox_env $distTarBall"
            }
          }
          inputs.files project.pythonSdkDeps
          outputs.files project.fileTree(dir: "${pythonRootDir}/target/.tox/${tox_env}/log/")
        }
      }

      // Run single or a set of integration tests with provided test options and pipeline options.
      project.ext.enablePythonPerformanceTest = {

        // Use the implicit it parameter of the closure to handle zero argument or one argument map calls.
        // See: http://groovy-lang.org/closures.html#implicit-it
        def config = it ? it as PythonPerformanceTestConfiguration : new PythonPerformanceTestConfiguration()

        project.task('integrationTest') {
          dependsOn 'installGcpTest'
          dependsOn ':sdks:python:sdist'

          doLast {
            def argMap = [:]

            // Build test options that configures test environment and framework
            def testOptions = []
            if (config.tests)
              testOptions += "--tests=$config.tests"
            if (config.attribute)
              testOptions += "--attr=$config.attribute"
            testOptions.addAll(config.extraTestOptions)
            argMap["test_opts"] = testOptions

            // Build pipeline options that configures pipeline job
            if (config.pipelineOptions)
              argMap["pipeline_opts"] = config.pipelineOptions
            if (config.kmsKeyName)
              argMap["kms_key_name"] = config.kmsKeyName
            argMap["suite"] = "integrationTest-perf"

            def cmdArgs = project.mapToArgString(argMap)
            def runScriptsDir = "${pythonRootDir}/scripts"
            project.exec {
              executable 'sh'
              args '-c', ". ${project.ext.envdir}/bin/activate && ${runScriptsDir}/run_integration_test.sh ${cmdArgs}"
            }
          }
        }
      }

      def addPortableWordCountTask = { boolean isStreaming, String runner ->
        def taskName = 'portableWordCount' + runner + (isStreaming ? 'Streaming' : 'Batch')
        project.task(taskName) {
          dependsOn = ['installGcpTest']
          mustRunAfter = [
            ':runners:flink:1.10:job-server:shadowJar',
            ':runners:spark:job-server:shadowJar',
            ':sdks:python:container:py2:docker',
            ':sdks:python:container:py35:docker',
            ':sdks:python:container:py36:docker',
            ':sdks:python:container:py37:docker'
          ]
          doLast {
            // TODO: Figure out GCS credentials and use real GCS input and output.
            def outputDir = File.createTempDir(taskName, '')
            def options = [
              "--input=/etc/profile",
              "--output=${outputDir}/out.txt",
              "--runner=${runner}",
              "--parallelism=2",
              "--sdk_worker_parallelism=1",
              "--flink_job_server_jar=${project.project(':runners:flink:1.10:job-server').shadowJar.archivePath}",
              "--spark_job_server_jar=${project.project(':runners:spark:job-server').shadowJar.archivePath}",
            ]
            if (isStreaming)
              options += [
                "--streaming"
              ]
            else
              // workaround for local file output in docker container
              options += [
                "--environment_cache_millis=60000"
              ]
            if (project.hasProperty("jobEndpoint"))
              options += [
                "--job_endpoint=${project.property('jobEndpoint')}"
              ]
            if (project.hasProperty("environmentType")) {
              options += [
                "--environment_type=${project.property('environmentType')}"
              ]
            }
            if (project.hasProperty("environmentConfig")) {
              options += [
                "--environment_config=${project.property('environmentConfig')}"
              ]
            }
            project.exec {
              executable 'sh'
              args '-c', ". ${project.ext.envdir}/bin/activate && python -m apache_beam.examples.wordcount ${options.join(' ')}"
              // TODO: Check that the output file is generated and runs.
            }
          }
        }
      }
      project.ext.addPortableWordCountTasks = {
        ->
        addPortableWordCountTask(false, "FlinkRunner")
        addPortableWordCountTask(true, "FlinkRunner")
        addPortableWordCountTask(false, "SparkRunner")
      }

      project.ext.getVersionSuffix = { String version ->
        return version == '2.7' ? '2' : version.replace('.', '')
      }

      project.ext.getVersionsAsList = { String propertyName ->
        return project.getProperty(propertyName).split(',')
      }
    }
  }

  private void setAutomaticModuleNameHeader(JavaNatureConfiguration configuration, Project project) {
    if (configuration.publish && !configuration.automaticModuleName) {
      throw new GradleException("Expected automaticModuleName to be set for the module that is published to maven repository.")
    } else if (configuration.automaticModuleName) {
      project.jar.manifest {
        attributes 'Automatic-Module-Name': configuration.automaticModuleName
      }
    }
  }
}
