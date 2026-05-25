import * as core from '@actions/core'
import * as exec from '@actions/exec'

import which from 'which'
import * as semver from 'semver'
import * as provisioner from './provision'
import * as gradlew from './gradlew'

export async function provisionAndMaybeExecute(
    gradleVersion: string,
    buildRootDirectory: string,
    args: string[]
): Promise<void> {
    // Download and install Gradle if required
    const executable = await provisioner.provisionGradle(gradleVersion)

    // Only execute if arguments have been provided
    if (args.length > 0) {
        await executeGradleBuild(executable, buildRootDirectory, args)
    }
}

async function executeGradleBuild(executable: string | undefined, root: string, args: string[]): Promise<void> {
    // Use the provided executable, or look for a Gradle wrapper script to run
    const toExecute = executable ?? gradlew.gradleWrapperScript(root)

    const status: number = await exec.exec(toExecute, args, {
        cwd: root,
        ignoreReturnCode: true
    })

    if (status !== 0) {
        core.setFailed(`Gradle build failed: see console output for details`)
    }
}

export function versionIsAtLeast(actualVersion: string, requiredVersion: string): boolean {
    if (actualVersion === requiredVersion) {
        return true
    }

    const actual = new GradleVersion(actualVersion)
    const required = new GradleVersion(requiredVersion)

    const actualSemver = semver.coerce(actual.versionPart)!
    const comparisonSemver = semver.coerce(required.versionPart)!

    if (semver.gt(actualSemver, comparisonSemver)) {
        return true // Actual version is greater than comparison. So it's at least as new.
    }
    if (semver.lt(actualSemver, comparisonSemver)) {
        return false // Actual version is less than comparison. So it's not as new.
    }

    // Actual and required version numbers are equal, so compare the other parts

    if (actual.snapshotPart || required.snapshotPart) {
        if (actual.snapshotPart && !required.snapshotPart && !required.stagePart) {
            return false // Actual has a snapshot, but required is a plain version. Required is newer.
        }
        if (required.snapshotPart && !actual.snapshotPart && !actual.stagePart) {
            return true // Required has a snapshot, but actual is a plain version. Actual is newer.
        }

        return false // Cannot compare case where both versions have a snapshot or stage
    }

    if (actual.stagePart) {
        if (required.stagePart) {
            return actual.stagePart >= required.stagePart // Compare stages for newer
        }

        return false // Actual has a stage, but required does not. So required is always newer.
    }

    return true // Actual has no stage part or snapshot part, so it cannot be older than required.
}

export async function findGradleExecutableOnPath(): Promise<string | null> {
    return await which('gradle', {nothrow: true})
}

export async function determineGradleVersion(gradleExecutable: string): Promise<string | undefined> {
    const output = await exec.getExecOutput(gradleExecutable, ['-v'], {silent: true})
    return parseGradleVersionFromOutput(output.stdout)
}

export function parseGradleVersionFromOutput(output: string): string | undefined {
    const regex = /Gradle (\d+\.\d+(\.\d+)?(-.*)?)/
    const versionString = output.match(regex)?.[1]
    return versionString
}

class GradleVersion {
    static PATTERN = /((\d+)(\.\d+)+)(-([a-z]+)-(\w+))?(-(SNAPSHOT|\d{14}([-+]\d{4})?))?/

    versionPart: string
    stagePart: string
    snapshotPart: string

    constructor(readonly version: string) {
        const matcher = GradleVersion.PATTERN.exec(version)
        if (!matcher) {
            throw new Error(`'${version}' is not a valid Gradle version string (examples: '1.0', '1.0-rc-1')`)
        }

        this.versionPart = matcher[1]
        this.stagePart = matcher[4]
        this.snapshotPart = matcher[7]
    }
}
