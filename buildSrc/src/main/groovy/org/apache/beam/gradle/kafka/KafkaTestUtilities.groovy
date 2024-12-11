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
package org.apache.beam.gradle.kafka

import org.gradle.api.Project
import org.gradle.api.artifacts.ConfigurationContainer
import org.gradle.api.tasks.testing.Test

import javax.inject.Inject

class KafkaTestUtilities {
  abstract static class KafkaBatchIT extends Test {

    @Inject
    KafkaBatchIT(String delimited, String undelimited, Boolean sdfCompatible, ConfigurationContainer configurations, Project runningProject){
      def kafkaioProject = runningProject.findProject(":sdks:java:io:kafka")
      group = "Verification"
      description = "Runs KafkaIO IT tests with Kafka clients API $delimited"
      outputs.upToDateWhen { false }
      testClassesDirs = runningProject.findProject(":sdks:java:io:kafka").sourceSets.test.output.classesDirs
      classpath = runningProject.sourceSets.test.runtimeClasspath + kafkaioProject.configurations."kafkaVersion$undelimited" + kafkaioProject.sourceSets.test.runtimeClasspath
      systemProperty "beam.target.kafka.version", delimited

      def pipelineOptions = [
        '--sourceOptions={' +
        '"numRecords": "1000",' +
        '"keySizeBytes": "10",' +
        '"valueSizeBytes": "90"' +
        '}',
        "--readTimeout=60",
        "--kafkaTopic=beam",
        "--withTestcontainers=true",
        "--kafkaContainerVersion=5.5.2",
      ]

      systemProperty "beamTestPipelineOptions", groovy.json.JsonOutput.toJson(pipelineOptions)
      include '**/KafkaIOIT.class'

      filter {
        excludeTestsMatching "*InStreaming"
        if (!sdfCompatible) {
          excludeTestsMatching "*DynamicPartitions" //admin client create partitions does not exist in kafka 0.11.0.3 and kafka sdf does not appear to work for kafka versions <2.0.1
          excludeTestsMatching "*SDFResumesCorrectly" //Kafka SDF does not work for kafka versions <2.0.1
          excludeTestsMatching "*StopReadingFunction" //Kafka SDF does not work for kafka versions <2.0.1
          excludeTestsMatching "*WatermarkUpdateWithSparseMessages" //Kafka SDF does not work for kafka versions <2.0.1
          excludeTestsMatching "*KafkaIOSDFReadWithErrorHandler"
        }
      }
    }
  }
}
