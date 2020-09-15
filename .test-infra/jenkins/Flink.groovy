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

class Flink {
  private static final String flinkDownloadUrl = 'https://archive.apache.org/dist/flink/flink-1.10.1/flink-1.10.1-bin-scala_2.11.tgz'
  private static final String hadoopDownloadUrl = 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-9.0/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar'
  private static final String FLINK_DIR = '"$WORKSPACE/src/.test-infra/dataproc"'
  private static final String FLINK_SCRIPT = 'flink_cluster.sh'
  private def job
  private String jobName

  Flink(job, String jobName) {
    this.job = job
    this.jobName = jobName
  }

  /**
   * Creates Flink cluster and specifies cleanup steps.
   *
   * @param sdkHarnessImages - the list of published SDK Harness images tags
   * @param workerCount - the initial number of worker nodes
   * @param jobServerImage -  the Flink job server image tag. If left empty, cluster will be set up without the job server.
   * @param slotsPerTaskmanager - the number of slots per Flink task manager
   */
  void setUp(List<String> sdkHarnessImages, Integer workerCount, String jobServerImage = '', Integer slotsPerTaskmanager = 1) {
    setupFlinkCluster(sdkHarnessImages, workerCount, jobServerImage, slotsPerTaskmanager)
    addTeardownFlinkStep()
  }

  private void setupFlinkCluster(List<String> sdkHarnessImages, Integer workerCount, String jobServerImage, Integer slotsPerTaskmanager) {
    String gcsBucket = 'gs://beam-flink-cluster'
    String clusterName = getClusterName()
    String artifactsDir = "${gcsBucket}/${clusterName}"
    String imagesToPull = sdkHarnessImages.join(' ')

    job.steps {
      environmentVariables {
        env("GCLOUD_ZONE", "us-central1-a")
        env("CLUSTER_NAME", clusterName)
        env("GCS_BUCKET", gcsBucket)
        env("FLINK_DOWNLOAD_URL", flinkDownloadUrl)
        env("HADOOP_DOWNLOAD_URL", hadoopDownloadUrl)
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
      shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} create")
    }
  }

  /**
   * Updates the number of worker nodes in a cluster.
   *
   * @param workerCount - the new number of worker nodes in the cluster
   */
  void scaleCluster(Integer workerCount) {
    job.steps {
      shell("echo Changing number of workers to ${workerCount}")
      environmentVariables {
        env("FLINK_NUM_WORKERS", workerCount)
      }
      shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} restart")
    }
  }

  private GString getClusterName() {
    return "${jobName.toLowerCase().replace("_", "-")}-\$BUILD_ID"
  }

  private void addTeardownFlinkStep() {
    job.publishers {
      postBuildScripts {
        steps {
          shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} delete")
        }
        onlyIfBuildSucceeds(false)
        onlyIfBuildFails(false)
      }
    }
  }
}
