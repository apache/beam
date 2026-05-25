import * as setupGradle from '../../setup-gradle'
import * as provisioner from '../../execution/provision'
import * as dependencyGraph from '../../dependency-graph'
import {
    BuildScanConfig,
    CacheConfig,
    DependencyGraphConfig,
    GradleExecutionConfig,
    WrapperValidationConfig,
    getActionId,
    setActionId
} from '../../configuration'
import {failOnUseOfRemovedFeature, saveDeprecationState} from '../../deprecation-collector'
import {handleMainActionError} from '../../errors'

/**
 * The main entry point for the action, called by Github Actions for the step.
 */
export async function run(): Promise<void> {
    try {
        if (getActionId() === 'gradle/gradle-build-action') {
            failOnUseOfRemovedFeature(
                'The action `gradle/gradle-build-action` has been replaced by `gradle/actions/setup-gradle`'
            )
        }

        setActionId('gradle/actions/setup-gradle')

        // Configure Gradle environment (Gradle User Home)
        await setupGradle.setup(new CacheConfig(), new BuildScanConfig(), new WrapperValidationConfig())

        // Configure the dependency graph submission
        await dependencyGraph.setup(new DependencyGraphConfig())

        const config = new GradleExecutionConfig()
        config.verifyNoArguments()
        await provisioner.provisionGradle(config.getGradleVersion())

        saveDeprecationState()
    } catch (error) {
        handleMainActionError(error)
    }

    // Explicit process.exit() to prevent waiting for hanging promises.
    process.exit()
}

run()
