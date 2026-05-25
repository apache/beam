import * as core from '@actions/core'
import * as exec from '@actions/exec'

import fs from 'fs'
import path from 'path'
import * as provisioner from '../execution/provision'
import {BuildResult, BuildResults} from '../build-results'
import {versionIsAtLeast} from '../execution/gradle'
import {gradleWrapperScript} from '../execution/gradlew'

export class CacheCleaner {
    private readonly gradleUserHome: string
    private readonly tmpDir: string

    constructor(gradleUserHome: string, tmpDir: string) {
        this.gradleUserHome = gradleUserHome
        this.tmpDir = tmpDir
    }

    async prepare(): Promise<string> {
        // Save the current timestamp
        const timestamp = Date.now().toString()
        core.saveState('clean-timestamp', timestamp)
        return timestamp
    }

    async forceCleanup(buildResults: BuildResults): Promise<void> {
        const executable = await this.gradleExecutableForCleanup(buildResults)
        const cleanTimestamp = core.getState('clean-timestamp')
        await this.forceCleanupFilesOlderThan(cleanTimestamp, executable)
    }

    /**
     * Attempt to use the newest Gradle version that was used to run a build, at least 8.11.
     *
     * This will avoid the need to provision a Gradle version for the cleanup when not necessary.
     */
    private async gradleExecutableForCleanup(buildResults: BuildResults): Promise<string> {
        const preferredVersion = buildResults.highestGradleVersion()
        if (preferredVersion && versionIsAtLeast(preferredVersion, '8.11')) {
            try {
                const wrapperScripts = buildResults.results
                    .map(result => this.findGradleWrapperScript(result))
                    .filter(Boolean) as string[]

                return await provisioner.provisionGradleWithVersionAtLeast(preferredVersion, wrapperScripts)
            } catch (_) {
                // Ignore the case where the preferred version cannot be located in https://services.gradle.org/versions/all.
                // This can happen for snapshot Gradle versions.
                core.info(
                    `Failed to provision Gradle ${preferredVersion} for cache cleanup. Falling back to default version.`
                )
            }
        }

        // Fallback to the minimum version required for cache-cleanup
        return await provisioner.provisionGradleWithVersionAtLeast('8.11')
    }

    private findGradleWrapperScript(result: BuildResult): string | null {
        try {
            const wrapperScript = gradleWrapperScript(result.rootProjectDir)
            return path.resolve(result.rootProjectDir, wrapperScript)
        } catch (error) {
            core.debug(`No Gradle Wrapper found for ${result.rootProjectName}: ${error}`)
            return null
        }
    }

    // Visible for testing
    async forceCleanupFilesOlderThan(cleanTimestamp: string, executable: string): Promise<void> {
        // Run a dummy Gradle build to trigger cache cleanup
        const cleanupProjectDir = path.resolve(this.tmpDir, 'dummy-cleanup-project')
        fs.mkdirSync(cleanupProjectDir, {recursive: true})
        fs.writeFileSync(
            path.resolve(cleanupProjectDir, 'settings.gradle'),
            'rootProject.name = "dummy-cleanup-project"'
        )
        fs.writeFileSync(
            path.resolve(cleanupProjectDir, 'init.gradle'),
            `
            beforeSettings { settings ->
                def cleanupTime = ${cleanTimestamp}
            
                settings.caches {
                    cleanup = Cleanup.ALWAYS
            
                    releasedWrappers.setRemoveUnusedEntriesOlderThan(cleanupTime)
                    snapshotWrappers.setRemoveUnusedEntriesOlderThan(cleanupTime)
                    downloadedResources.setRemoveUnusedEntriesOlderThan(cleanupTime)
                    createdResources.setRemoveUnusedEntriesOlderThan(cleanupTime)
                    buildCache.setRemoveUnusedEntriesOlderThan(cleanupTime)
                }
            }
            `
        )
        fs.writeFileSync(path.resolve(cleanupProjectDir, 'build.gradle'), 'task("noop") {}')

        await core.group('Executing Gradle to clean up caches', async () => {
            core.info(`Cleaning up caches last used before ${cleanTimestamp}`)
            await this.executeCleanupBuild(executable, cleanupProjectDir)
        })
    }

    private async executeCleanupBuild(executable: string, cleanupProjectDir: string): Promise<void> {
        const args = [
            '-g',
            this.gradleUserHome,
            '-I',
            'init.gradle',
            '--info',
            '--no-daemon',
            '--no-scan',
            '--build-cache',
            '-DGITHUB_DEPENDENCY_GRAPH_ENABLED=false',
            '-DGRADLE_ACTIONS_SKIP_BUILD_RESULT_CAPTURE=true',
            'noop'
        ]

        await exec.exec(executable, args, {
            cwd: cleanupProjectDir
        })
    }
}
