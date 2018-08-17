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

job('beam_Reproducible_Env_Test') {
    description('Runs Beam Reproducible Environment Test.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Run Reproducible Env Test',
            'Run Reproducible Env Test')

    environmentVariables {
        env('JAVA_HOME', '/docker-java-home')
    }

    wrappers {
        buildInDocker {
            dockerfile('src/.test-infra/dockerized-jenkins/build-env/', 'Dockerfile')
        }
        colorizeOutput()
    }
    def machine = "beam6"
    parameters {
        nodeParam('TEST_HOST') {
            description("Select test host ${machine}")
            defaultNodes([machine])
            allowedNodes([machine])
            trigger('multiSelectionDisallowed')
            eligibility('IgnoreOfflineNodeEligibility')
        }
    }
    steps {
        shell('pwd && cd src && echo $UID && echo $USER && pip --version && ./gradlew :pythonPreCommit --info --continue')
//        shell('pwd && cd src && echo $UID && echo $USER && pip --version && python .test-infra/jenkins/userid_test.py')
//        gradle {
//            rootBuildScriptDir(commonJobProperties.checkoutDir)
//            tasks(':pythonPreCommit')
//            commonJobProperties.setGradleSwitches(delegate)
//        }
    }

}
