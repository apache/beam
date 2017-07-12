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

List<Object> ghprbArgs = [
    string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
    string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
    string(name: 'ghprbPullId', value: "${ghprbPullId}")
]

List<Object> sha1Arg = [string(name: 'sha1', value: "origin/pr/${ghprbPullId}/head")]

int javaBuildNum = -1
stage('Build') {
    parallel (
        java: {
            def javaBuild = build job: 'beam_PreCommit_Java_Build', parameters: sha1Arg + ghprbArgs
            if(javaBuild.getResult() == Result.SUCCESS.toString()) {
                javaBuildNum = javaBuild.getNumber()
            }
        }
    )
}

javaBuildArg = [string(name: 'buildNum', value: "${javaBuildNum}")]
javaUnitPassed = false
stage('Unit Test / Code Health') {
    parallel (
        java_unit: {
            if(javaBuildNum != -1) {
                def javaTest = build job: 'beam_PreCommit_Java_UnitTest', parameters: javaBuildArg + ghprbArgs
                if(javaTest.getResult() == Result.SUCCESS.toString()) {
                    javaUnitPassed = true
                }
            }
        },
        java_codehealth: {
            if(javaBuildNum != -1) {
                build job: 'beam_PreCommit_Java_CodeHealth', parameters: javaBuildArg + ghprbArgs
            }
        },
        python_unit: {
            build job: 'beam_PreCommit_Python_UnitTest', parameters: sha1Arg + ghprbArgs
        }
    )
}

stage('Integration Test') {
    parallel (
        java_integration: {
            if(javaUnitPassed) {
                build job: 'beam_PreCommit_Java_IntegrationTest', parameters: javaBuildArg + ghprbArgs
            }
        }
    )
}
