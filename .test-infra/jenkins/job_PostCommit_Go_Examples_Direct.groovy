import CommonJobProperties as commonJobProperties

// This job runs the Go examples tests with Direct runner.
job('beam_PostCommit_Go_Examples_Direct') {
    description('Runs the Go Examples with DirectRunner.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 120)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Go Direct Runner Examples',
            'Run Go Examples_Direct')

    // Execute shell command to run examples.
    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(":goPostCommitDirect")
            commonJobProperties.setGradleSwitches(delegate)
            switches('--no-parallel')
        }
    }
}