#!groovy
import hudson.model.Result

try {
    javaBuildNum = -1
    pythonBuildNum = -1
    stage('Build') {
        parallel (
            java: {
                def javaBuild = build job: 'beam_PreCommit_Java_Build', parameters: [
                    string(name: 'sha1', value: "origin/pr/${ghprbPullId}/head"),
                    string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                    string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                    string(name: 'ghprbPullId', value: "${ghprbPullId}")
                ]
                if(javaBuild.getResult() == Result.SUCCESS.toString()) {
                    javaBuildNum = javaBuild.getNumber()
                }
            },
            python: {
                def pythonBuild = build job: 'beam_PreCommit_Python_Build', parameters:[
                    string(name: 'sha1', value: "origin/pr/${ghprbPullId}/head"),
                    string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                    string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                    string(name: 'ghprbPullId', value: "${ghprbPullId}")
                ]
                if(pythonBuild.getResult() == Result.SUCCESS.toString()) {
                    pythonBuildNum = pythonBuild.getNumber()
                }
            }
        )
    }
    javaUnitPassed = false
    pythonUnitPassed = false
    stage('Unit Test / Code Health') {
        parallel (
            java_unit: {
                if(javaBuildNum != -1) {
                    def javaTest = build job: 'beam_PreCommit_Java_UnitTest', parameters: [
                        string(name: 'buildNum', value: "${javaBuildNum}"),
                        string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                        string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                        string(name: 'ghprbPullId', value: "${ghprbPullId}")
                    ]
                    if(javaTest.getResult() == Result.SUCCESS.toString()) {
                        javaUnitPassed = true
                    }
                }
            },
            java_codehealth: {
                if(javaBuildNum != -1) {
                    build job: 'beam_PreCommit_Java_CodeHealth', parameters: [
                        string(name: 'buildNum', value: "${javaBuildNum}"),
                        string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                        string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                        string(name: 'ghprbPullId', value: "${ghprbPullId}")
                    ]
                }
            },
            python_unit: {
                if(pythonBuildNum != -1) {
                    def pythonTest = build job: 'beam_PreCommit_Python_UnitTest', parameters: [
                        string(name: 'buildNum', value: "${pythonBuildNum}"),
                        string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                        string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                        string(name: 'ghprbPullId', value: "${ghprbPullId}")
                    ]
                    if(pythonTest.getResult() == Result.SUCCESS.toString()) {
                        pythonUnitPassed = true
                    }
                }
            }
        )
    }
    stage('Integration Test') {
        parallel (
            java_integration: {
                if(javaUnitPassed) {
                    build job: 'beam_PreCommit_Java_IntegrationTest', parameters: [
                string(name: 'buildNum', value: "${javaBuildNum}"),
                string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                string(name: 'ghprbPullId', value: "${ghprbPullId}")
            ]
                }
            },
            python_integration: {
                if(pythonUnitPassed) {
                    build job: 'beam_PreCommit_Python_IntegrationTest', parameters: [
                        string(name: 'buildNum', value: "${pythonBuildNum}"),
                        string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
                        string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
                        string(name: 'ghprbPullId', value: "${ghprbPullId}")
                    ]
                }
            }
        )
    }
} catch (Exception e) {
    echo e.toString()
}
