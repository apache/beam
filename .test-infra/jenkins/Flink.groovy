/*
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

import CommonJobProperties as common
import CommonTestProperties.SDK

class Flink {
  private static final String repositoryRoot = 'gcr.io/apache-beam-testing/beam_portability'
  private static final String dockerTag = 'latest'
  private static final String jobServerImageTag = "${repositoryRoot}/flink-job-server:${dockerTag}"
  private static final String flinkVersion = '1.7'
  private static final String flinkDownloadUrl = 'https://archive.apache.org/dist/flink/flink-1.7.0/flink-1.7.0-bin-hadoop28-scala_2.11.tgz'
  private def job
  private String jobName

  Flink(job, String jobName) {
    this.job = job
    this.jobName = jobName
  }

 /**
  * Returns SDK Harness image tag to be used as an environment_config in the job definition.
  *
  * @param sdk - SDK
  */
  static String getSDKHarnessImageTag(SDK sdk) {
    switch (sdk) {
      case CommonTestProperties.SDK.PYTHON:
        return "${repositoryRoot}/python:${dockerTag}"
      case CommonTestProperties.SDK.JAVA:
        return "${repositoryRoot}/java:${dockerTag}"
      default:
        String sdkName = sdk.name().toLowerCase()
        throw new IllegalArgumentException("${sdkName} SDK is not supported")
    }
  }

  /**
   * Creates Flink cluster and specifies cleanup steps.
   *
   * @param sdk - SDK
   * @param workerCount - the initial number of worker nodes
   * @param slotsPerTaskmanager - the number of slots per Flink task manager
   */
  void setUp(SDK sdk, Integer workerCount, Integer slotsPerTaskmanager = 1) {
    setupFlinkCluster(sdk, workerCount, slotsPerTaskmanager)
    addTeardownFlinkStep()
  }

  /**
   * Updates the number of worker nodes in a cluster.
   *
   * @param workerCount - the new number of worker nodes in the cluster
   */
  void scaleCluster(Integer workerCount) {
    job.steps {
      environmentVariables {
        env("FLINK_NUM_WORKERS", workerCount)
      }
      shell("gcloud dataproc clusters delete ${getClusterName(jobName)} --quiet")

      shell('echo Setting up flink cluster')
      shell("cd ${common.makePathAbsolute('src/.test-infra/dataproc/')}; ./create_flink_cluster.sh")
    }
  }

  void prepareSDKHarness(SDK sdk) {
    job.steps {
      String sdkName = sdk.name().toLowerCase()
      String image = "${repositoryRoot}/${sdkName}"
      String imageTag = "${image}:${dockerTag}"

      shell("echo \"Building SDK harness for ${sdkName} SDK.\"")
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        tasks(":sdks:${sdkName}:container:docker")
        switches("-Pdocker-repository-root=${repositoryRoot}")
        switches("-Pdocker-tag=${dockerTag}")
      }
      shell("echo \"Tagging Harness' image\"...")
      shell("docker tag ${image} ${imageTag}")
      shell("echo \"Pushing Harness' image\"...")
      shell("docker push ${imageTag}")
    }
  }

  void prepareJobServer() {
    job.steps {
      String image = "${repositoryRoot}/flink-job-server"
      String imageTag = "${image}:${dockerTag}"

      shell('echo "Building Flink job Server"')

      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        tasks(":runners:flink:${flinkVersion}:job-server-container:docker")
        switches("-Pdocker-repository-root=${repositoryRoot}")
        switches("-Pdocker-tag=${dockerTag}")
      }

      shell("echo \"Tagging Flink Job Server's image\"...")
      shell("docker tag ${image} ${imageTag}")
      shell("echo \"Pushing Flink Job Server's image\"...")
      shell("docker push ${imageTag}")
    }
  }

  private void setupFlinkCluster(SDK sdk, Integer workerCount, Integer slotsPerTaskmanager) {
    String gcsBucket = 'gs://beam-flink-cluster'
    String clusterName = getClusterName()
    String artifactsDir = "${gcsBucket}/${clusterName}"
    String imagesToPull = getSDKHarnessImageTag(sdk)

    job.steps {
      environmentVariables {
        env("GCLOUD_ZONE", "us-central1-a")
        env("CLUSTER_NAME", clusterName)
        env("GCS_BUCKET", gcsBucket)
        env("FLINK_DOWNLOAD_URL", flinkDownloadUrl)
        env("FLINK_NUM_WORKERS", workerCount)
        env("FLINK_TASKMANAGER_SLOTS", slotsPerTaskmanager)
        env("DETACHED_MODE", 'true')

        if(imagesToPull) {
          env("HARNESS_IMAGES_TO_PULL", imagesToPull)
        }

        if(jobServerImageTag) {
          env("JOB_SERVER_IMAGE", jobServerImageTag)
          env("ARTIFACTS_DIR", artifactsDir)
        }
      }

      shell('echo Setting up flink cluster')
      shell("cd ${common.makePathAbsolute('src/.test-infra/dataproc/')}; ./create_flink_cluster.sh")
    }
  }

  private void addTeardownFlinkStep() {
    job.publishers {
      postBuildScripts {
        steps {
          shell("gcloud dataproc clusters delete ${getClusterName()} --quiet")
        }
        onlyIfBuildSucceeds(false)
        onlyIfBuildFails(false)
      }
    }
  }

  private GString getClusterName() {
    return "${jobName.toLowerCase().replace("_", "-")}-\$BUILD_ID"
  }
}
