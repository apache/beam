import * as setupGradle from '../../setup-gradle'
import * as dependencyGraph from '../../dependency-graph'

import {CacheConfig, DependencyGraphConfig, SummaryConfig} from '../../configuration'
import {handlePostActionError} from '../../errors'
import {emitDeprecationWarnings, restoreDeprecationState} from '../../deprecation-collector'

// Catch and log any unhandled exceptions.  These exceptions can leak out of the uploadChunk method in
// @actions/toolkit when a failed upload closes the file descriptor causing any in-process reads to
// throw an uncaught exception.  Instead of failing this action, just warn.
process.on('uncaughtException', e => handlePostActionError(e))

/**
 * The post-execution entry point for the action, called by Github Actions after completing all steps for the Job.
 */
export async function run(): Promise<void> {
    try {
        restoreDeprecationState()
        emitDeprecationWarnings()

        if (await setupGradle.complete(new CacheConfig(), new SummaryConfig())) {
            // Only submit the dependency graphs once per job
            await dependencyGraph.complete(new DependencyGraphConfig())
        }
    } catch (error) {
        handlePostActionError(error)
    }

    // Explicit process.exit() to prevent waiting for promises left hanging by `@actions/cache` on save.
    process.exit()
}

run()
