import common_job_properties

// Defines a job.
mavenJob('beam_PreCommit_Java_MavenInstall') {
  description('Runs an install of the current GitHub Pull Request.')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate)

  // Set pull request build trigger.
  common_job_properties.setPullRequestBuildTrigger(
      delegate,
      'Jenkins: Maven clean install')

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Set spark env variables for ITs.
  common_job_properties.setSparkEnvVariables(delegate)
  
  goals('-B -e -Prelease,include-runners,jenkins-precommit,direct-runner,dataflow-runner,spark-runner,flink-runner,apex-runner -DrepoToken=${COVERALLS_REPO_TOKEN} -DpullRequest=99999 help:effective-settings clean install coveralls:report')
}
