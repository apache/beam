import common_job_properties

// Defines a job.
mavenJob('beam_PostCommit_Java_MavenInstall') {
  description('Runs an install of the current GitHub Pull Request.')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate)

  // Set maven paramaters.
  common_job_properties.setMavenConfig(delegate)

  // Set build triggers
  triggers {
    cron('0 */6 * * *')
    scm('* * * * *')
  }
  
  goals('-B -e -P release,dataflow-runner -DrepoToken=${COVERALLS_REPO_TOKEN} clean install coveralls:report -DskipITs=false -DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests", "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner" ]\'')
}
