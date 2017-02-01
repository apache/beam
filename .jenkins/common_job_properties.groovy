// Contains functions that help build Jenkins projects. Functions typically set
// common properties that are shared among all Jenkins projects.
class common_job_properties {

  // Sets common top-level job properties.
  static def setTopLevelJobProperties(def context) {

    // GitHub project.
    context.properties {
      githubProjectUrl('https://github.com/apache/beam-site/')
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
          url('https://github.com/apache/beam-site.git')
          refspec('+refs/heads/*:refs/remotes/origin/* ' +
                  '+refs/pull/*:refs/remotes/origin/pr/*')
        }
        branch('${sha1}')
        extensions {
          cleanAfterCheckout()
        }
      }
    }

    context.parameters {
      // This is a recommended setup if you want to run the job manually. The
      // ${sha1} parameter needs to be provided, and defaults to the main branch.
      stringParam(
          'sha1',
          'asf-site',
          'Commit id or refname (eg: origin/pr/9/head) you want to build.')
    }

    context.wrappers {
      // Abort the build if it's stuck for more minutes than specified.
      timeout {
        absolute(30)
        abortBuild()
      }
    }
  }

  // Sets the pull request build trigger.
  static def setPullRequestBuildTrigger(def context,
                                        def commitStatusContext,
                                        def successComment = '--none--') {
    context.triggers {
      githubPullRequest {
        admins(['asfbot'])
        useGitHubHooks()
        orgWhitelist(['apache'])
        allowMembersOfWhitelistedOrgsAsAdmin()
        permitAll()

        extensions {
          commitStatus {
            // This is the name that will show up in the GitHub pull request UI
            // for this Jenkins project.
            delegate.context(commitStatusContext)
          }

          /*
            This section is disabled, because of jenkinsci/ghprb-plugin#417 issue.
            For the time being, an equivalent configure section below is added.

          // Comment messages after build completes.
          buildStatus {
            completedStatus('SUCCESS', successComment)
            completedStatus('FAILURE', '--none--')
            completedStatus('ERROR', '--none--')
          }
          */
        }
      }
    }

    // Comment messages after build completes.
    context.configure {
      def messages = it / triggers / 'org.jenkinsci.plugins.ghprb.GhprbTrigger' / extensions / 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildStatus' / messages
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message(successComment)
        result('SUCCESS')
      }
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message('--none--')
        result('ERROR')
      }
      messages << 'org.jenkinsci.plugins.ghprb.extensions.comments.GhprbBuildResultMessage' {
        message('--none--')
        result('FAILURE')
      }
    }
  }
}
