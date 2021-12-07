import CommonJobProperties as commonJobProperties

// This job runs the Go examples tests with Flink runner.
job('beam_PostCommit_Go_Examples_Flink') {
    description('Runs the Go Examples with FlinkRunner.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 120)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Go Flink Runner Examples',
            'Run Go Examples_Flink')

    // Execute shell command to run examples.
    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(":goPostCommitFlink")
            commonJobProperties.setGradleSwitches(delegate)
            switches('--no-parallel')
        }
    }
}