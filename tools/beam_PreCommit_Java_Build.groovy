evaluate(new File(
         new File(getClass().protectionDomain.codeSource.location.path).parent,
         "common_job_properties.groovy"))

// Defines a job.
mavenJob('beam_PreCommit_Java_Build') {
  description('Runs a compile of the current GitHub Pull Request.')

  // Set common parameters.
  commonTopLevelJobProperties(delegate)

  triggers {
    // GitHub Pull Request builder.
    githubPullRequest {
      commonGitHubPullRequestBuilder(delegate)
      extensions {
        commitStatus {
          // The name to show this build in GitHub pull request.
          context('Jenkins: Maven clean compile.')
        }
        // Messages after build completes.
        buildStatus {
          completedStatus('SUCCESS', '\nJenkins successfully built Beam at commit id ' +
              '${ghprbActualCommit}.')
          completedStatus('FAILURE', '--none--')
          completedStatus('ERROR', '--none--')
        }
      }
    }
  }
  
  goals('clean compile')
}
