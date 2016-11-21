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
  
  goals('-B -e -Prelease,include-runners,jenkins-precommit,direct-runner,dataflow-runner,spark-runner,flink-runner,apex-runner -DrepoToken=${COVERALLS_REPO_TOKEN} -DpullRequest=${ghprbPullId} help:effective-settings clean install coveralls:report')
}
