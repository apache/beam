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

class Infrastructure {

  static void prepareSDKHarness(def context, SDK sdk, String repositoryRoot, String dockerTag) {
    context.steps {
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

  static void prepareFlinkJobServer(def context, String flinkVersion, String repositoryRoot, String dockerTag) {
    context.steps {
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

  static void setupFlinkCluster(def context, String clusterNamePrefix, String flinkDownloadUrl, String imagesToPull, String jobServerImage, Integer workerCount, Integer slotsPerTaskmanager = 1) {
    String gcsBucket = 'gs://beam-flink-cluster'
    String clusterName = getClusterName(clusterNamePrefix)
    String artifactsDir="${gcsBucket}/${clusterName}"

    context.steps {
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

        if(jobServerImage) {
          env("JOB_SERVER_IMAGE", jobServerImage)
          env("ARTIFACTS_DIR", artifactsDir)
        }
      }

      shell('echo Setting up flink cluster')
      shell("cd ${common.makePathAbsolute('src/.test-infra/dataproc/')}; ./create_flink_cluster.sh")
    }
  }

  static void teardownDataproc(def context, String jobName) {
    context.publishers {
      postBuildScripts {
        steps {
          shell("gcloud dataproc clusters delete ${getClusterName(jobName)} --quiet")
        }
        onlyIfBuildSucceeds(false)
        onlyIfBuildFails(false)
      }
    }
  }

  static void scaleCluster(def context, String jobName, Integer workerCount) {
    context.steps {
      environmentVariables {
        env("FLINK_NUM_WORKERS", workerCount)
      }
      shell("gcloud dataproc clusters delete ${getClusterName(jobName)} --quiet")

      shell('echo Setting up flink cluster')
      shell("cd ${common.makePathAbsolute('src/.test-infra/dataproc/')}; ./create_flink_cluster.sh")
    }
  }

  private static GString getClusterName(String jobName) {
    return "${jobName.toLowerCase().replace("_", "-")}-\$BUILD_ID"
  }
}
