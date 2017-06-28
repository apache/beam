#!groovy
import hudson.model.Result

try {
    def javaBuildNum = -1
    def pythonBuildNum = -1
    stage('Build') {
        parallel (
            java: {
                def javaBuild = build job: 'beam_PreCommit_Java_Build', parameters: [string(name: 'prNum', value: "${ghprbPullId}")]
                if(javaBuild.getResult() == Result.SUCCESS.toString()) {
                    javaBuildNum = javaBuild.getNumber()
                }
            },
            python: {
                def pythonBuild = build job: 'beam_PreCommit_Python_Build', parameters:[string(name: 'prNum', value: "${ghprbPullId}")]
                if(pythonBuild.getResult() == Result.SUCCESS.toString()) {
                    pythonBuildNum = pythonBuild.getNumber()
                }
            }
        )
    }
    def javaUnitPassed = false
    def pythonUnitPassed = false
    stage('Unit Test / Code Health') {
        parallel (
            java_unit: {
                if(javaBuildNum != -1) {
                    def javaTest = build job: 'beam_PreCommit_Java_UnitTest', parameters: [string(name: 'buildNum', value: javaBuildNum)]
                    if(javaTest.getResult() == Result.SUCCESS.toString()) {
                        javaUnitPassed = true
                    }
                }
            },
            java_codehealth: {
                if(javaBuildNum != -1) {
                    build job: 'beam_PreCommit_Java_CodeHealth', parameters: [string(name: 'buildNum', value: javaBuildNum)]
                }
            },
            python_unit: {
                if(pythonBuildNum != -1) {
                    def pythonTest = build job: 'beam_PreCommit_Python_UnitTest', parameters: [string(name: 'buildNum', value: pythonBuildNum)]
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
                    build job: 'beam_PreCommit_Java_IntegrationTest', parameters: [string(name: 'buildNum', value: javaBuildNum)]
                }
            },
            python_integration: {
                if(pythonUnitPassed) {
                    build job: 'beam_PreCommit_Python_IntegrationTest', parameters: [string(name: 'buildNum', value: pythonBuildNum)]
                }
            }
        )
    }
} catch (Exception e) {
    echo e.toString()
}
