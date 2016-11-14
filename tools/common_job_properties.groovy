// This file contains functions that help build Jenkins projects. Functions
// typically set common properties that are shared among all Jenkins projects.

// Sets common top-level job properties.
def commonTopLevelJobProperties(def context) {

  // GitHub project.
  context.properties {
    githubProjectUrl('https://github.com/apache/incubator-beam/')
  }

  // Restrict this project to run only on Jenkins executors dedicated to the
  // Apache Beam project.
  context.label('beam')

  // Discard old builds. Build records are only kept up to this number of days.
  context.logRotator {
    daysToKeep(14)
  }

  // Source code management.
  context.scm {
    git {
      remote {
        url('https://github.com/apache/incubator-beam.git')
        refspec('+refs/heads/*:refs/remotes/origin/* ' +
                '+refs/pull/*:refs/remotes/origin/pr/*')
      }
      branch('${sha1}')
      extensions {
        cleanAfterCheckout()
      }
    }
  }

  context.wrappers {
    // Abort the build if it's stuck for more minutes than specified.
    timeout {
      absolute(30)
      abortBuild()
    }
  }
}

// Sets GitHub pull request builder properties.
def commonGitHubPullRequestBuilder(def context) {
  context.admins(['asfbot'])
  context.useGitHubHooks()
  context.orgWhitelist(['apache'])
  context.allowMembersOfWhitelistedOrgsAsAdmin()
  context.permitAll()
}
