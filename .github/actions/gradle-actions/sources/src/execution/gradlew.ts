import * as core from '@actions/core'
import * as path from 'path'
import fs from 'fs'

const IS_WINDOWS = process.platform === 'win32'

export function wrapperScriptFilename(): string {
    return IS_WINDOWS ? './gradlew.bat' : './gradlew'
}

export function installScriptFilename(): string {
    return IS_WINDOWS ? 'gradle.bat' : 'gradle'
}

export function gradleWrapperScript(buildRootDirectory: string): string {
    validateGradleWrapper(buildRootDirectory)
    return wrapperScriptFilename()
}

function validateGradleWrapper(buildRootDirectory: string): void {
    const wrapperScript = path.resolve(buildRootDirectory, wrapperScriptFilename())
    verifyExists(wrapperScript, 'Gradle Wrapper script')
    verifyIsExecutableScript(wrapperScript)

    const wrapperProperties = path.resolve(buildRootDirectory, 'gradle/wrapper/gradle-wrapper.properties')
    verifyExists(wrapperProperties, 'Gradle wrapper properties file')
}

function verifyExists(file: string, description: string): void {
    if (!fs.existsSync(file)) {
        throw new Error(
            `Cannot locate ${description} at '${file}'. Specify 'gradle-version' for projects without Gradle wrapper configured.`
        )
    }
}

function verifyIsExecutableScript(toExecute: string): void {
    try {
        fs.accessSync(toExecute, fs.constants.X_OK)
    } catch (err) {
        core.warning(
            `Gradle wrapper script '${toExecute}' is not executable. Action will set executable permission and continue.`
        )
        fs.chmodSync(toExecute, '755')
    }
}
