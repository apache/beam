#!groovy

try {
    stage('Build') {
        node {
            checkout scm
            def mvnHome = tool 'maven-3'
            sh "${mvnHome}/bin/mvn clean install -DskipTests -Dcheckstyle.skip -Dfindbugs.skip"
            stash 'all'
        }
    }
    
    stage('Test') {
        parallel unitTest: {
            node {
                unstash 'all'
                def mvnHome = tool 'maven-3'
                echo '$ghprbPullId'
                echo "$ghprbPullId"
                echo $ghprbPullId
                sh "${mvnHome}/bin/mvn test"
            }
        },
        codeStyle: {
            node {
                unstash 'all'
                def mvnHome = tool 'maven-3'
                sh "${mvnHome}/bin/mvn checkstyle:check findbugs:check"
            }
        }
    }
} catch (Exception e) {
    echo e
}
