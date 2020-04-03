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

  private static def job

  private static String kubeconfigLocation

  private static String namespace

  private Kubernetes(job, String kubeconfigLocation, String namespace) {
    this.job = job
    this.kubeconfigLocation = kubeconfigLocation
    this.namespace = namespace
  }

  /**
   * Creates separate kubeconfig, kubernetes namespace and specifies related cleanup steps.
   *
   * @param job - jenkins job
   * @param kubeconfigLocation - place where kubeconfig will be created
   * @param namepsace - kubernetes namespace
   */
  static Kubernetes create(job, String kubeconfigLocation, String namespace) {
    Kubernetes kubernetes = new Kubernetes(job, kubeconfigLocation, namespace)
    setupKubeconfig()
    setupNamespace()
    addCleanupSteps()
    return kubernetes
  }

  private static void setupKubeconfig() {
    job.steps {
      shell("cp /home/jenkins/.kube/config ${kubeconfigLocation}")
      environmentVariables {
        env('KUBECONFIG', kubeconfigLocation)
      }
    }
  }

  private static void setupNamespace() {
    job.steps {
      shell("${KUBERNETES_SCRIPT} createNamespace ${namespace}")
      environmentVariables {
        env('KUBERNETES_NAMESPACE', namespace)
      }
    }
  }

  private static void addCleanupSteps() {
    job.publishers {
      postBuildScripts {
        steps {
          shell("${KUBERNETES_SCRIPT} deleteNamespace ${namespace}")
          shell("rm ${kubeconfigLocation}")
        }
        onlyIfBuildSucceeds(false)
        onlyIfBuildFails(false)
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
}
