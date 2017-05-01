#!groovy

try {
    stage('Build') {
        node {
            checkout scm
            def mvnHome = tool 'maven-3'
            sh "${mvnHome}/bin/mvn clean install -DskipTests -Dcheckstyle.skip -Dfindbugs.skip -Dmaven.javadoc.skip -Drat.skip"
            stash 'all'
        }
    }
    
    stage('Verify') {
        parallel unitTest: {
            node {
                unstash 'all'
                def mvnHome = tool 'maven-3'
                sh "${mvnHome}/bin/mvn test"
            }
        }, codeStyle: {
            node {
                unstash 'all'
                def mvnHome = tool 'maven-3'
                sh "${mvnHome}/bin/mvn checkstyle:check findbugs:check rat:check"
            }
        }, javadoc: {
            node {
                unstash 'all'
                def mvnHome = tool 'maven-3'
                sh "${mvnHome}/bin/mvn javadoc:jar"
            }
        }       
    }
    
    stage('Integration Test') {
        node {
            unstash 'all'
            def mvnHome = tool 'maven-3'
            sh "${mvnHome}/bin/mvn compile test-compile failsafe:ingegration-test -pl examples/java -am -Prelease,include-runners,jenkins-precommit,direct-runner,dataflow-runner,spark-runner,flink-runner,apex-runner"
        }
    }
} catch (Exception e) {
    echo e
}
