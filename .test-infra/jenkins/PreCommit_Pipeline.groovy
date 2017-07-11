#!groovy
import hudson.model.Result

def ghprbArgs = [
    string(name: 'ghprbGhRepository', value: "${ghprbGhRepository}"),
    string(name: 'ghprbActualCommit', value: "${ghprbActualCommit}"),
    string(name: 'ghprbPullId', value: "${ghprbPullId}")
]

def sha1Arg = [string(name: 'sha1', value: "origin/pr/${ghprbPullId}/head")]

try {
    javaBuildNum = -1
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
} catch (Exception e) {
    echo e.toString()
}
