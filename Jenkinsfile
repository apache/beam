#!groovy
try {
    stage('Build') {
        node {
            checkout scm
            def mvnHome = tool 'maven-3'
            sh "${mvnHome}/bin/mvn clean install -DskipTests -Dcheckstyle.skip -Dfindbugs.skip"
        }
    }
    
    stage('Test') {
        parallel unitTest: {
            node {
                unstash 'all'
                echo 'ut'
                sh 'ls -l'
            }
        },
        codeStyle: {
            node {
                unstash 'all'
                echo 'cs'
                sh 'ls -al'
            }
        }
    }
} catch (Exception e) {
    echo e
}
