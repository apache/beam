import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import * as httpm from '@actions/http-client'
import * as core from '@actions/core'
import * as cache from '@actions/cache'
import * as toolCache from '@actions/tool-cache'

import {determineGradleVersion, findGradleExecutableOnPath, versionIsAtLeast} from './gradle'
import * as gradlew from './gradlew'
import {handleCacheFailure} from '../caching/cache-utils'
import {CacheConfig} from '../configuration'

const gradleVersionsBaseUrl = 'https://services.gradle.org/versions'

/**
 * Install any configured version of Gradle, adding the executable to the PATH.
 * @return Installed Gradle executable or undefined if no version configured.
 */
export async function provisionGradle(gradleVersion: string): Promise<string | undefined> {
    if (gradleVersion !== '' && gradleVersion !== 'wrapper') {
        return addToPath(await installGradle(gradleVersion))
    }

    return undefined
}

async function addToPath(executable: string): Promise<string> {
    core.addPath(path.dirname(executable))
    return executable
}

async function installGradle(version: string): Promise<string> {
    const versionInfo = await resolveGradleVersion(version)
    core.setOutput('gradle-version', versionInfo.version)
    return installGradleVersion(versionInfo)
}

async function resolveGradleVersion(version: string): Promise<GradleVersionInfo> {
    switch (version) {
        case 'current':
            return gradleCurrent()
        case 'rc':
            core.warning(`Specifying gradle-version 'rc' has been deprecated. Use 'release-candidate' instead.`)
            return gradleReleaseCandidate()
        case 'release-candidate':
            return gradleReleaseCandidate()
        case 'nightly':
            return gradleNightly()
        case 'release-nightly':
            return gradleReleaseNightly()
        default:
            return gradleRelease(version)
    }
}

async function gradleCurrent(): Promise<GradleVersionInfo> {
    return await gradleVersionDeclaration(`${gradleVersionsBaseUrl}/current`)
}

async function gradleReleaseCandidate(): Promise<GradleVersionInfo> {
    const versionInfo = await gradleVersionDeclaration(`${gradleVersionsBaseUrl}/release-candidate`)
    if (versionInfo && versionInfo.version && versionInfo.downloadUrl) {
        return versionInfo
    }
    core.info('No current release-candidate found, will fallback to current')
    return gradleCurrent()
}

async function gradleNightly(): Promise<GradleVersionInfo> {
    return await gradleVersionDeclaration(`${gradleVersionsBaseUrl}/nightly`)
}

async function gradleReleaseNightly(): Promise<GradleVersionInfo> {
    return await gradleVersionDeclaration(`${gradleVersionsBaseUrl}/release-nightly`)
}

async function gradleRelease(version: string): Promise<GradleVersionInfo> {
    const versionInfo = await findGradleVersionDeclaration(version)
    if (!versionInfo) {
        throw new Error(`Gradle version ${version} does not exist`)
    }
    return versionInfo
}

async function gradleVersionDeclaration(url: string): Promise<GradleVersionInfo> {
    return await httpGetGradleVersion(url)
}

async function findGradleVersionDeclaration(version: string): Promise<GradleVersionInfo | undefined> {
    const gradleVersions = await httpGetGradleVersions(`${gradleVersionsBaseUrl}/all`)
    return gradleVersions.find((entry: GradleVersionInfo) => {
        return entry.version === version
    })
}

async function installGradleVersion(versionInfo: GradleVersionInfo): Promise<string> {
    return core.group(`Provision Gradle ${versionInfo.version}`, async () => {
        const gradleOnPath = await findGradleExecutableOnPath()
        if (gradleOnPath) {
            const gradleOnPathVersion = await determineGradleVersion(gradleOnPath)
            if (gradleOnPathVersion === versionInfo.version) {
                core.info(`Gradle version ${versionInfo.version} is already available on PATH. Not installing.`)
                return gradleOnPath
            }
        }

        return locateGradleAndDownloadIfRequired(versionInfo)
    })
}

/**
 * Find (or install) a Gradle executable that meets the specified version requirement.
 * The Gradle version on PATH and all candidates are first checked for version compatibility.
 * If no existing Gradle version meets the requirement, the required version is installed.
 * @return Gradle executable with at least the required version.
 */
export async function provisionGradleWithVersionAtLeast(
    minimumVersion: string,
    candidates: string[] = []
): Promise<string> {
    const gradleOnPath = await findGradleExecutableOnPath()
    const allCandidates = gradleOnPath ? [gradleOnPath, ...candidates] : candidates

    return core.group(`Provision Gradle >= ${minimumVersion}`, async () => {
        for (const candidate of allCandidates) {
            const candidateVersion = await determineGradleVersion(candidate)
            if (candidateVersion && versionIsAtLeast(candidateVersion, minimumVersion)) {
                core.info(
                    `Gradle version ${candidateVersion} is available at ${candidate} and >= ${minimumVersion}. Not installing.`
                )
                return candidate
            }
        }

        return locateGradleAndDownloadIfRequired(await gradleRelease(minimumVersion))
    })
}

async function locateGradleAndDownloadIfRequired(versionInfo: GradleVersionInfo): Promise<string> {
    const installsDir = path.join(getProvisionDir(), 'installs')
    const installDir = path.join(installsDir, `gradle-${versionInfo.version}`)
    if (fs.existsSync(installDir)) {
        core.info(`Gradle installation already exists at ${installDir}`)
        return executableFrom(installDir)
    }

    const downloadPath = await downloadAndCacheGradleDistribution(versionInfo)
    await toolCache.extractZip(downloadPath, installsDir)
    core.info(`Extracted Gradle ${versionInfo.version} to ${installDir}`)

    const executable = executableFrom(installDir)
    fs.chmodSync(executable, '755')
    core.info(`Provisioned Gradle executable ${executable}`)

    return executable
}

async function downloadAndCacheGradleDistribution(versionInfo: GradleVersionInfo): Promise<string> {
    const downloadPath = path.join(getProvisionDir(), `downloads/gradle-${versionInfo.version}-bin.zip`)

    // TODO: Convert this to a class and inject config
    const cacheConfig = new CacheConfig()
    if (cacheConfig.isCacheDisabled()) {
        await downloadGradleDistribution(versionInfo, downloadPath)
        return downloadPath
    }

    const cacheKey = `gradle-${versionInfo.version}`
    try {
        const restoreKey = await cache.restoreCache([downloadPath], cacheKey)
        if (restoreKey) {
            core.info(`Restored Gradle distribution ${cacheKey} from cache to ${downloadPath}`)
            return downloadPath
        }
    } catch (error) {
        handleCacheFailure(error, `Restore Gradle distribution ${versionInfo.version} failed`)
    }

    core.info(`Gradle distribution ${versionInfo.version} not found in cache. Will download.`)
    await downloadGradleDistribution(versionInfo, downloadPath)

    if (!cacheConfig.isCacheReadOnly()) {
        try {
            await cache.saveCache([downloadPath], cacheKey)
        } catch (error) {
            handleCacheFailure(error, `Save Gradle distribution ${versionInfo.version} failed`)
        }
    }
    return downloadPath
}

function getProvisionDir(): string {
    const tmpDir = process.env['RUNNER_TEMP'] ?? os.tmpdir()
    return path.join(tmpDir, `.gradle-actions/gradle-installations`)
}

async function downloadGradleDistribution(versionInfo: GradleVersionInfo, downloadPath: string): Promise<void> {
    await toolCache.downloadTool(versionInfo.downloadUrl, downloadPath)
    core.info(`Downloaded ${versionInfo.downloadUrl} to ${downloadPath} (size ${fs.statSync(downloadPath).size})`)
}

function executableFrom(installDir: string): string {
    return path.join(installDir, 'bin', `${gradlew.installScriptFilename()}`)
}

async function httpGetGradleVersion(url: string): Promise<GradleVersionInfo> {
    return JSON.parse(await httpGetString(url))
}

async function httpGetGradleVersions(url: string): Promise<GradleVersionInfo[]> {
    return JSON.parse(await httpGetString(url))
}

async function httpGetString(url: string): Promise<string> {
    const httpClient = new httpm.HttpClient('gradle/actions')
    const response = await httpClient.get(url)
    return response.readBody()
}

interface GradleVersionInfo {
    version: string
    downloadUrl: string
}
