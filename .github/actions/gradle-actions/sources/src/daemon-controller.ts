import * as core from '@actions/core'
import * as exec from '@actions/exec'
import * as fs from 'fs'
import * as path from 'path'
import {BuildResults} from './build-results'

export class DaemonController {
    private readonly gradleHomes

    constructor(buildResults: BuildResults) {
        this.gradleHomes = buildResults.uniqueGradleHomes()
    }

    async stopAllDaemons(): Promise<void> {
        const executions: Promise<number>[] = []
        const args = ['--stop']

        for (const gradleHome of this.gradleHomes) {
            const executable = path.resolve(gradleHome, 'bin', 'gradle')
            if (!fs.existsSync(executable)) {
                core.warning(`Gradle executable not found at ${executable}. Could not stop Gradle daemons.`)
                continue
            }
            core.info(`Stopping Gradle daemons for ${gradleHome}`)
            executions.push(
                exec.exec(executable, args, {
                    ignoreReturnCode: true
                })
            )
        }
        await Promise.all(executions)
    }
}
