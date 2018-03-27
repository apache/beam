pipeline {
    agent { docker { image 'google/cloud-sdk:latest' } }
    stages {
        stage('test') {
            steps {
                sh 'release/src/main/groovy/run_release_candidate_python_mobile_gaming.sh'
            }
        }
    }
}