import * as core from '@actions/core'
import * as exec from '@actions/exec'
import * as glob from '@actions/glob'

import path from 'path'
import fs from 'fs'
import {generateCacheKey} from './cache-key'
import {CacheListener} from './cache-reporting'
import {saveCache, restoreCache, cacheDebug, isCacheDebuggingEnabled, tryDelete} from './cache-utils'
import {CacheConfig, ACTION_METADATA_DIR} from '../configuration'
import {GradleHomeEntryExtractor, ConfigurationCacheEntryExtractor} from './gradle-home-extry-extractor'
import {getPredefinedToolchains, mergeToolchainContent, readResourceFileAsString} from './gradle-user-home-utils'

const RESTORED_CACHE_KEY_KEY = 'restored-cache-key'

export class GradleUserHomeCache {
    private readonly cacheName = 'home'
    private readonly cacheDescription = 'Gradle User Home'

    private readonly userHome: string
    private readonly gradleUserHome: string
    private readonly cacheConfig: CacheConfig

    constructor(userHome: string, gradleUserHome: string, cacheConfig: CacheConfig) {
        this.userHome = userHome
        this.gradleUserHome = gradleUserHome
        this.cacheConfig = cacheConfig
    }

    init(): void {
        this.initializeGradleUserHome()

        // Export the GRADLE_ENCRYPTION_KEY variable if provided
        const encryptionKey = this.cacheConfig.getCacheEncryptionKey()
        if (encryptionKey) {
            core.exportVariable('GRADLE_ENCRYPTION_KEY', encryptionKey)
        }
    }

    cacheOutputExists(): boolean {
        const cachesDir = path.resolve(this.gradleUserHome, 'caches')
        if (fs.existsSync(cachesDir)) {
            cacheDebug(`Cache output exists at ${cachesDir}`)
            return true
        }
        return false
    }

    /**
     * Restores the cache entry, finding the closest match to the currently running job.
     */
    async restore(listener: CacheListener): Promise<void> {
        const entryListener = listener.entry(this.cacheDescription)

        const cacheKey = generateCacheKey(this.cacheName, this.cacheConfig)

        cacheDebug(
            `Requesting ${this.cacheDescription} with
    key:${cacheKey.key}
    restoreKeys:[${cacheKey.restoreKeys}]`
        )

        const cachePath = this.getCachePath()
        const cacheResult = await restoreCache(cachePath, cacheKey.key, cacheKey.restoreKeys, entryListener)
        if (!cacheResult) {
            core.info(`${this.cacheDescription} cache not found. Will initialize empty.`)
            return
        }

        core.saveState(RESTORED_CACHE_KEY_KEY, cacheResult.key)

        try {
            await this.afterRestore(listener)
        } catch (error) {
            core.warning(`Restore ${this.cacheDescription} failed in 'afterRestore': ${error}`)
        }
    }

    /**
     * Restore any extracted cache entries after the main Gradle User Home entry is restored.
     */
    async afterRestore(listener: CacheListener): Promise<void> {
        await this.debugReportGradleUserHomeSize('as restored from cache')
        await new GradleHomeEntryExtractor(this.gradleUserHome, this.cacheConfig).restore(listener)
        await new ConfigurationCacheEntryExtractor(this.gradleUserHome, this.cacheConfig).restore(listener)
        await this.deleteExcludedPaths()
        await this.debugReportGradleUserHomeSize('after restoring common artifacts')
    }

    /**
     * Saves the cache entry based on the current cache key unless the cache was restored with the exact key,
     * in which case we cannot overwrite it.
     *
     * If the cache entry was restored with a partial match on a restore key, then
     * it is saved with the exact key.
     */
    async save(listener: CacheListener): Promise<void> {
        const cacheKey = generateCacheKey(this.cacheName, this.cacheConfig).key
        const restoredCacheKey = core.getState(RESTORED_CACHE_KEY_KEY)
        const gradleHomeEntryListener = listener.entry(this.cacheDescription)

        if (restoredCacheKey && cacheKey === restoredCacheKey) {
            core.info(`Cache hit occurred on the cache key ${cacheKey}, not saving cache.`)

            for (const entryListener of listener.cacheEntries) {
                if (entryListener === gradleHomeEntryListener) {
                    entryListener.markNotSaved('cache key not changed')
                } else {
                    entryListener.markNotSaved(`referencing '${this.cacheDescription}' cache entry not saved`)
                }
            }
            return
        }

        try {
            await this.beforeSave(listener)
        } catch (error) {
            core.warning(`Save ${this.cacheDescription} failed in 'beforeSave': ${error}`)
            return
        }

        const cachePath = this.getCachePath()
        await saveCache(cachePath, cacheKey, gradleHomeEntryListener)
        return
    }

    /**
     * Extract and save any defined extracted cache entries prior to the main Gradle User Home entry being saved.
     */
    async beforeSave(listener: CacheListener): Promise<void> {
        await this.debugReportGradleUserHomeSize('before saving common artifacts')
        await this.deleteExcludedPaths()
        await Promise.all([
            new GradleHomeEntryExtractor(this.gradleUserHome, this.cacheConfig).extract(listener),
            new ConfigurationCacheEntryExtractor(this.gradleUserHome, this.cacheConfig).extract(listener)
        ])
        await this.debugReportGradleUserHomeSize(
            "after extracting common artifacts (only 'caches' and 'notifications' will be stored)"
        )
    }

    /**
     * Delete any file paths that are excluded by the `gradle-home-cache-excludes` parameter.
     */
    private async deleteExcludedPaths(): Promise<void> {
        const rawPaths: string[] = this.cacheConfig.getCacheExcludes()
        rawPaths.push('caches/*/cc-keystore')
        const resolvedPaths = rawPaths.map(x => path.resolve(this.gradleUserHome, x))

        for (const p of resolvedPaths) {
            cacheDebug(`Removing excluded path: ${p}`)
            const globber = await glob.create(p, {
                implicitDescendants: false
            })

            for (const toDelete of await globber.glob()) {
                cacheDebug(`Removing excluded file: ${toDelete}`)
                await tryDelete(toDelete)
            }
        }
    }

    /**
     * Determines the paths within Gradle User Home to cache.
     * By default, this is the 'caches' and 'notifications' directories,
     * but this can be overridden by the `gradle-home-cache-includes` parameter.
     */
    protected getCachePath(): string[] {
        const rawPaths: string[] = this.cacheConfig.getCacheIncludes()
        rawPaths.push(ACTION_METADATA_DIR)
        const resolvedPaths = rawPaths.map(x => this.resolveCachePath(x))
        cacheDebug(`Using cache paths: ${resolvedPaths}`)
        return resolvedPaths
    }

    private resolveCachePath(rawPath: string): string {
        if (rawPath.startsWith('!')) {
            const resolved = this.resolveCachePath(rawPath.substring(1))
            return `!${resolved}`
        }
        return path.resolve(this.gradleUserHome, rawPath)
    }

    private initializeGradleUserHome(): void {
        // Create a directory for storing action metadata
        const actionCacheDir = path.resolve(this.gradleUserHome, ACTION_METADATA_DIR)
        fs.mkdirSync(actionCacheDir, {recursive: true})

        this.copyInitScripts()

        // Copy the default toolchain definitions to `~/.m2/toolchains.xml`
        this.registerToolchains()

        if (core.isDebug()) {
            this.configureInfoLogLevel()
        }
    }

    private copyInitScripts(): void {
        // Copy init scripts from src/resources to Gradle UserHome
        const initScriptsDir = path.resolve(this.gradleUserHome, 'init.d')
        fs.mkdirSync(initScriptsDir, {recursive: true})
        const initScriptFilenames = [
            'gradle-actions.build-result-capture.init.gradle',
            'gradle-actions.build-result-capture-service.plugin.groovy',
            'gradle-actions.github-dependency-graph.init.gradle',
            'gradle-actions.github-dependency-graph-gradle-plugin-apply.groovy',
            'gradle-actions.inject-develocity.init.gradle'
        ]
        for (const initScriptFilename of initScriptFilenames) {
            const initScriptContent = readResourceFileAsString('init-scripts', initScriptFilename)
            const initScriptPath = path.resolve(initScriptsDir, initScriptFilename)
            fs.writeFileSync(initScriptPath, initScriptContent)
        }
    }

    private registerToolchains(): void {
        const preInstalledToolchains: string | null = getPredefinedToolchains()
        if (preInstalledToolchains == null) return

        const m2dir = path.resolve(this.userHome, '.m2')
        const toolchainXmlTarget = path.resolve(m2dir, 'toolchains.xml')
        if (!fs.existsSync(toolchainXmlTarget)) {
            // Write a new toolchains.xml file if it doesn't exist
            fs.mkdirSync(m2dir, {recursive: true})
            fs.writeFileSync(toolchainXmlTarget, preInstalledToolchains)

            core.info(`Wrote default JDK locations to ${toolchainXmlTarget}`)
        } else {
            // Merge into an existing toolchains.xml file
            const existingToolchainContent = fs.readFileSync(toolchainXmlTarget, 'utf8')
            const mergedContent = mergeToolchainContent(existingToolchainContent, preInstalledToolchains)

            fs.writeFileSync(toolchainXmlTarget, mergedContent)
            core.info(`Merged default JDK locations into ${toolchainXmlTarget}`)
        }
    }

    /**
     * When the GitHub environment ACTIONS_RUNNER_DEBUG is true, run Gradle with --info and --stacktrace.
     * see https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/enabling-debug-logging
     *
     * @VisibleForTesting
     */
    configureInfoLogLevel(): void {
        const infoProperties = `org.gradle.logging.level=info\norg.gradle.logging.stacktrace=all\n`
        const propertiesFile = path.resolve(this.gradleUserHome, 'gradle.properties')
        if (fs.existsSync(propertiesFile)) {
            core.info(`Merged --info and --stacktrace into existing ${propertiesFile} file`)
            const existingProperties = fs.readFileSync(propertiesFile, 'utf-8')
            fs.writeFileSync(propertiesFile, `${infoProperties}\n${existingProperties}`)
        } else {
            core.info(`Created a new ${propertiesFile} with --info and --stacktrace`)
            fs.writeFileSync(propertiesFile, infoProperties)
        }
    }

    /**
     * When cache debugging is enabled (or ACTIONS_STEP_DEBUG is on),
     * this method will give a detailed report of the Gradle User Home contents.
     */
    private async debugReportGradleUserHomeSize(label: string): Promise<void> {
        if (!isCacheDebuggingEnabled() && !core.isDebug()) {
            return
        }
        if (!fs.existsSync(this.gradleUserHome)) {
            return
        }
        const result = await exec.getExecOutput('du', ['-h', '-c', '-t', '5M'], {
            cwd: this.gradleUserHome,
            silent: true,
            ignoreReturnCode: true
        })

        core.info(`Gradle User Home (directories >5M): ${label}`)

        core.info(
            result.stdout
                .trimEnd()
                .replace(/\t/g, '    ')
                .split('\n')
                .map(it => {
                    return `  ${it}`
                })
                .join('\n')
        )

        core.info('-----------------------')
    }
}
