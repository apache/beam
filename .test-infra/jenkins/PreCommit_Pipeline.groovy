#!groovy
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

import hudson.model.Result

int NO_BUILD = -1

// These are args for the GitHub Pull Request Builder (ghprb) Plugin. Providing these arguments is
// necessary due to a bug in the ghprb plugin where environment variables are not correctly passed
// to jobs downstream of a Pipeline job.
// Tracked by https://github.com/jenkinsci/ghprb-plugin/issues/572.
List<Object> ghprbArgs = [
    string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
    string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
    string(name: 'ghprbPullId', value: "${ghprbPullId}")
]

// This argument is the commit at which to build.
List<Object> commitArg = [string(name: 'sha1', value: "origin/pr/${ghprbPullId}/head")]

int javaBuildNum = NO_BUILD

final String JAVA_BUILD_TYPE = "java"
final String PYTHON_BUILD_TYPE = "python"
final String ALL_BUILD_TYPE = "all"

def buildTypes = [
        JAVA_BUILD_TYPE,
        PYTHON_BUILD_TYPE,
        ALL_BUILD_TYPE,
]

String currentBuildType = ALL_BUILD_TYPE
String commentLower = ghprbCommentBody.toLowerCase()

// Currently if there is nothing selected (e.g. the comment is just "retest this please") we select "all" by default.
// In the future we should provide some mechanism, either via commenting or the suite failure message, to enforce
// selection of one of the build types.
if (!commentLower.isEmpty()) {
    commentSplit = commentLower.split(' ')
    buildType = commentSplit[commentSplit.length-1]
    if (buildTypes.contains(buildType)) {
        currentBuildType = buildType
    }
}

// This (and the below) define "Stages" of a pipeline. These stages run serially, and inside can
// have "parallel" blocks which execute several work steps concurrently. This work is limited to
// simple operations -- more complicated operations need to be performed on an actual node. In this
// case we are using the pipeline to trigger downstream builds.
stage('Build') {
    parallel (
        java: {
            if (currentBuildType == JAVA_BUILD_TYPE || currentBuildType == ALL_BUILD_TYPE) {
                def javaBuild = build job: 'beam_Java_Build', parameters: commitArg + ghprbArgs
                if (javaBuild.getResult() == Result.SUCCESS.toString()) {
                    javaBuildNum = javaBuild.getNumber()
                }
            } else {
                echo 'Skipping Java due to comment selecting non-Java execution: ' + ghprbCommentBody
            }
        },
        python_unit: { // Python doesn't have a build phase, so we include this here.
            if (currentBuildType == PYTHON_BUILD_TYPE || currentBuildType == ALL_BUILD_TYPE) {
                try {
                    build job: 'beam_Python_UnitTest', parameters: commitArg + ghprbArgs
                } catch (Exception e) {
                    echo 'Python build failed: ' + e.toString()
                }
            } else {
                echo 'Skipping Python due to comment selecting non-Python execution: ' + ghprbCommentBody
            }
        }
    )
}

// This argument is provided to downstream jobs so they know from which build to pull artifacts.
javaBuildArg = [string(name: 'buildNum', value: "${javaBuildNum}")]
javaUnitPassed = false

stage('Unit Test / Code Health') {
    parallel (
        java_unit: {
            if(javaBuildNum != NO_BUILD) {
                def javaTest = build job: 'beam_Java_UnitTest', parameters: javaBuildArg + ghprbArgs
                if(javaTest.getResult() == Result.SUCCESS.toString()) {
                    javaUnitPassed = true
                }
            }
        },
        java_codehealth: {
            if(javaBuildNum != NO_BUILD) {
                try {
                    build job: 'beam_Java_CodeHealth', parameters: javaBuildArg + ghprbArgs
                } catch (Exception e) {
                    echo 'Java CodeHealth Build Failed: ' + e.toString()
                }
            }
        }
    )
}

stage('Integration Test') {
    parallel (
        // Not gated on codehealth because codehealth shouldn't affect whether tests provide useful
        // signal.
        java_integration: {
            if(javaUnitPassed) {
                build job: 'beam_Java_IntegrationTest', parameters: javaBuildArg + ghprbArgs
            }
        }
    )
}
