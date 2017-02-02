import common_job_properties

// Defines a job.
job('beam_PreCommit_Website_Test') {
  description('Runs tests on the pull requests proposed for the Apache Beam ' +
              'website.')

  // Set common parameters.
  common_job_properties.setTopLevelWebsiteJobProperties(delegate)

  // Execute concurrent builds. Multiple builds of this project may be executed
  // in parallel. This is safe because this build does not require exclusive
  // access to any shared resources.
  concurrentBuild()

  // Set pull request build trigger.
  common_job_properties.setPreCommit(
      delegate,
      'Jenkins: test website (dead links, etc.)')

  steps {
    // Run the following shell script as a build step.
    shell '''
        # Install RVM.
        gpg --keyserver hkp://keys.gnupg.net --recv-keys \\
            409B6B1796C275462A1703113804BB82D39DC0E3
        \\curl -sSL https://get.rvm.io | bash
        source /home/jenkins/.rvm/scripts/rvm

        # Install Ruby.
        RUBY_VERSION_NUM=2.3.0
        rvm install ruby $RUBY_VERSION_NUM --autolibs=read-only

        # Install Bundler gem
        PATH=~/.gem/ruby/$RUBY_VERSION_NUM/bin:$PATH
        GEM_PATH=~/.gem/ruby/$RUBY_VERSION_NUM/:$GEM_PATH
        gem install bundler --user-install

        # Install all needed gems.
        bundle install --path ~/.gem/

        # Build the new site and test it.
        rm -fr ./content/
        bundle exec rake test
    '''.stripIndent().trim()
  }
}
