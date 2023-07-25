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
/** Facilitates creation of jenkins steps to setup and cleanup Kubernetes infrastructure. */
class Kubernetes {

  private static final String KUBERNETES_DIR = '"$WORKSPACE/src/.test-infra/kubernetes"'

  private static final String KUBERNETES_SCRIPT = "${KUBERNETES_DIR}/kubernetes.sh"

  private static final String DEFAULT_CLUSTER = 'io-datastores'

  private static def job

  private static String kubeconfigLocation

  private static String namespace

  private static String cluster

  private Kubernetes(job, String kubeconfigLocation, String namespace, String cluster) {
    this.job = job
    this.kubeconfigLocation = kubeconfigLocation
    this.namespace = namespace
    this.cluster = cluster
  }

  /**
   * Creates separate kubeconfig, kubernetes namespace and specifies related cleanup steps.
   *
   * @param job - jenkins job
   * @param kubeconfigLocation - place where kubeconfig will be created
   * @param namespace - kubernetes namespace. If empty, the default namespace will be used
   * @param cluster - name of the cluster to get credentials for
   */
  static Kubernetes create(job, String kubeconfigLocation, String namespace = '',
      String cluster = DEFAULT_CLUSTER) {
    Kubernetes kubernetes = new Kubernetes(job, kubeconfigLocation, namespace, cluster)
    setupKubeconfig()
    setupNamespace()
    addCleanupSteps()
    return kubernetes
  }

  private static void setupKubeconfig() {
    job.steps {
      shell("gcloud container clusters get-credentials ${cluster} --zone=us-central1-a")
      shell("cp /home/jenkins/.kube/config ${kubeconfigLocation}")
      environmentVariables {
        env('KUBECONFIG', kubeconfigLocation)
      }
    }
  }

  private static void setupNamespace() {
    if (!namespace.isEmpty()) {
      job.steps {
        shell("${KUBERNETES_SCRIPT} createNamespace ${namespace}")
        environmentVariables {
          env('KUBERNETES_NAMESPACE', namespace)
        }
      }
    }
  }

  private static void addCleanupSteps() {
    job.publishers {
      postBuildScript {
        buildSteps {
          postBuildStep {
            stopOnFailure(false)
            results([
              'FAILURE',
              'SUCCESS',
              'UNSTABLE',
              'NOT_BUILT',
              'ABORTED'
            ])
            buildSteps {
              if (!namespace.isEmpty()) {
                shell {
                  command("${KUBERNETES_SCRIPT} deleteNamespace ${namespace}")
                }
              }
              shell {
                command("rm ${kubeconfigLocation}")
              }
            }
          }
        }
        markBuildUnstable(false)
      }
    }
  }

  /**
   * Specifies steps to run Kubernetes .yaml script.
   */
  void apply(String pathToScript) {
    job.steps {
      shell("${KUBERNETES_SCRIPT} apply ${pathToScript}")
    }
  }

  /**
   * Specifies steps that will save specified load balancer serivce address
   * as an environment variable that can be used in later steps if needed.
   *
   * @param serviceName - name of the load balancer Kubernetes service
   * @param referenceName - name of the environment variable
   */
  void loadBalancerIP(String serviceName, String referenceName) {
    job.steps {
      String command = "${KUBERNETES_SCRIPT} loadBalancerIP ${serviceName}"
      shell("set -eo pipefail; eval ${command} | sed 's/^/${referenceName}=/' > job.properties")
      environmentVariables {
        propertiesFile('job.properties')
      }
    }
  }

  /**
   * Specifies steps that will return an available port on the Kubernetes cluster,
   * the value of the available port will be stored in job.properties using referenceName as key
   *
   * @param lowRangePort - low range port to be used
   * @param highRangePort - high range port to be used
   * @param referenceName - name of the environment variable
   */
  void availablePort(String lowRangePort, String highRangePort, String referenceName) {
    job.steps {
      String command = "${KUBERNETES_SCRIPT} getAvailablePort ${lowRangePort} ${highRangePort}"
      shell("set -xo pipefail; eval ${command} | sed 's/^/${referenceName}=/' > job.properties")
      environmentVariables {
        propertiesFile('job.properties')
      }
    }
  }

  /**
   * Specifies steps to wait until a job finishes
   * @param jobName - job running in Kubernetes cluster
   * @param timeout - max time to wait for job to finish
   */
  void waitForJob(String jobName, String timeout){
    job.steps{
      String command="${KUBERNETES_SCRIPT} waitForJob ${jobName} ${timeout}"
      shell("eval ${command}")
    }
  }
}
