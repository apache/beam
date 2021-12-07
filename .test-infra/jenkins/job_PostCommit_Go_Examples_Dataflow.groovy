import CommonJobProperties as commonJobProperties

// This job runs the Go examples tests with Dataflow runner.
job('beam_PostCommit_Go_Examples_Dataflow') {
    description('Runs the Go Examples with DataflowRunner.')

    // Set common parameters.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 120)

    // Allows triggering this build against pull requests.
    commonJobProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Go Dataflow Runner Examples',
            'Run Go Examples_Dataflow')

    // Execute shell command to run examples.
    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(":goPostCommitDataflow")
            commonJobProperties.setGradleSwitches(delegate)
            switches('--no-parallel')
        }
    }
}