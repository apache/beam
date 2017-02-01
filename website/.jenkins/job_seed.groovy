import common_job_properties

// Defines the seed job, which creates or updates all other Jenkins projects.
job('beam_SeedJob_Website') {
  description('Automatically configures all Apache Beam website Jenkins ' +
              'projects based on Jenkins DSL groovy files checked into the ' +
              'code repository.')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate)

  // Run this job every night to revert back any accidental changes to the
  // configuration.
  triggers {
    cron('0 6 * * *')
  }

  steps {
    dsl {
      // A list or a glob of other groovy files to process.
      external('.jenkins/job_*.groovy')

      // If a job is removed from the script, disable it (rather than deleting).
      removeAction('DISABLE')
    }
  }

  publishers {
    // Notify the mailing list for each failed build.
    mailer('dev@beam.apache.org', false, false)
  }
}
