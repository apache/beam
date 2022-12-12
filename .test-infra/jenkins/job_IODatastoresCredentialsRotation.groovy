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

import CommonJobProperties as commonJobProperties

job('Rotate IO-Datastores Cluster Credentials') {
  description('Rotates Certificates and performs an IP rotation for Metrics cluster')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate)

  // Sets that this is a cron job.
  commonJobProperties.setCronJob(delegate, 'H 2 1 * *')// At 00:02am every month.
  def date = new Date().format('E MMM dd HH:mm:ss z yyyy')

  steps {
    //Starting credential rotation
    shell('''gcloud container clusters update io-datastores \
    --start-credential-rotation --zone=us-central1-a --quiet''')

    //Rebuilding the nodes
    shell('''gcloud container clusters upgrade io-datastores \
    --node-pool=pool-1 --zone=us-central1-a --quiet''')

    //Completing the rotation
    shell('''gcloud container clusters update io-datastores \
    --complete-credential-rotation --zone=us-central1-a --quiet''')
  }

  publishers {
    extendedEmail {
      triggers {
        failure {
          subject('Credentials Rotation Failure on IO-Datastores cluster')
          content("Something went wrong during the automatic credentials rotation for IO-Datastores Cluster, performed at ${date}. It may be necessary to check the state of the cluster certificates. For further details refer to the following links:\n * Failing job: https://ci-beam.apache.org/job/Rotate%20IO-Datastores%20Cluster%20Credentials/ \n * Job configuration: https://github.com/apache/beam/blob/master/.test-infra/jenkins/job_IODatastoresCredentialsRotation.groovy \n * Cluster URL: https://pantheon.corp.google.com/kubernetes/clusters/details/us-central1-a/io-datastores/details?mods=dataflow_dev&project=apache-beam-testing")
          recipientList('dev@beam.apache.org')
        }
      }
    }
  }
}
