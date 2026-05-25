import * as core from '@actions/core'
import {
    CacheListener,
    EXISTING_GRADLE_HOME,
    CLEANUP_DISABLED_DUE_TO_FAILURE,
    CLEANUP_DISABLED_DUE_TO_CONFIG_CACHE_HIT
} from './cache-reporting'
import {GradleUserHomeCache} from './gradle-user-home-cache'
import {CacheCleaner} from './cache-cleaner'
import {DaemonController} from '../daemon-controller'
import {CacheConfig} from '../configuration'
import {BuildResults} from '../build-results'

const CACHE_RESTORED_VAR = 'GRADLE_BUILD_ACTION_CACHE_RESTORED'

export async function restore(
    userHome: string,
    gradleUserHome: string,
    cacheListener: CacheListener,
    cacheConfig: CacheConfig
): Promise<void> {
    // Bypass restore cache on all but first action step in workflow.
    if (process.env[CACHE_RESTORED_VAR]) {
        core.info('Cache only restored on first action step.')
        return
    }
    core.exportVariable(CACHE_RESTORED_VAR, true)

    const gradleStateCache = new GradleUserHomeCache(userHome, gradleUserHome, cacheConfig)

    if (cacheConfig.isCacheDisabled()) {
        core.info('Cache is disabled: will not restore state from previous builds.')
        // Initialize the Gradle User Home even when caching is disabled.
        gradleStateCache.init()
        cacheListener.setDisabled()
        return
    }

    if (gradleStateCache.cacheOutputExists()) {
        if (!cacheConfig.isCacheOverwriteExisting()) {
            core.info('Gradle User Home already exists: will not restore from cache.')
            // Initialize pre-existing Gradle User Home.
            gradleStateCache.init()
            cacheListener.setDisabled(EXISTING_GRADLE_HOME)
            return
        }
        core.info('Gradle User Home already exists: will overwrite with cached contents.')
    }

    gradleStateCache.init()
    // Mark the state as restored so that post-action will perform save.
    core.saveState(CACHE_RESTORED_VAR, true)

    if (cacheConfig.isCacheCleanupEnabled()) {
        core.info('Preparing cache for cleanup.')
        const cacheCleaner = new CacheCleaner(gradleUserHome, process.env['RUNNER_TEMP']!)
        await cacheCleaner.prepare()
    }

    if (cacheConfig.isCacheWriteOnly()) {
        core.info('Cache is write-only: will not restore from cache.')
        cacheListener.setWriteOnly()
        return
    }

    await core.group('Restore Gradle state from cache', async () => {
        await gradleStateCache.restore(cacheListener)
    })
}

export async function save(
    userHome: string,
    gradleUserHome: string,
    cacheListener: CacheListener,
    daemonController: DaemonController,
    buildResults: BuildResults,
    cacheConfig: CacheConfig
): Promise<void> {
    if (cacheConfig.isCacheDisabled()) {
        core.info('Cache is disabled: will not save state for later builds.')
        return
    }

    if (!core.getState(CACHE_RESTORED_VAR)) {
        core.info('Cache will not be saved: not restored in main action step.')
        return
    }

    if (cacheConfig.isCacheReadOnly()) {
        core.info('Cache is read-only: will not save state for use in subsequent builds.')
        cacheListener.setReadOnly()
        return
    }

    await core.group('Stopping Gradle daemons', async () => {
        await daemonController.stopAllDaemons()
    })

    if (cacheConfig.isCacheCleanupEnabled()) {
        if (buildResults.anyConfigCacheHit()) {
            core.info('Not performing cache-cleanup due to config-cache reuse')
            cacheListener.setCacheCleanupDisabled(CLEANUP_DISABLED_DUE_TO_CONFIG_CACHE_HIT)
        } else if (cacheConfig.shouldPerformCacheCleanup(buildResults.anyFailed())) {
            cacheListener.setCacheCleanupEnabled()
            await performCacheCleanup(gradleUserHome, buildResults)
        } else {
            core.info('Not performing cache-cleanup due to build failure')
            cacheListener.setCacheCleanupDisabled(CLEANUP_DISABLED_DUE_TO_FAILURE)
        }
    }

    await core.group('Caching Gradle state', async () => {
        return new GradleUserHomeCache(userHome, gradleUserHome, cacheConfig).save(cacheListener)
    })
}

async function performCacheCleanup(gradleUserHome: string, buildResults: BuildResults): Promise<void> {
    const cacheCleaner = new CacheCleaner(gradleUserHome, process.env['RUNNER_TEMP']!)
    try {
        await cacheCleaner.forceCleanup(buildResults)
    } catch (e) {
        core.warning(`Cache cleanup failed. Will continue. ${String(e)}`)
    }
}
