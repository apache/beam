import * as cache from '@actions/cache'

export const DEFAULT_CACHE_ENABLED_REASON = `[Cache was enabled](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#caching-build-state-between-jobs). Action attempted to both restore and save the Gradle User Home.`

export const DEFAULT_READONLY_REASON = `[Cache was read-only](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#using-the-cache-read-only). By default, the action will only write to the cache for Jobs running on the default branch.`

export const DEFAULT_DISABLED_REASON = `[Cache was disabled](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#disabling-caching) via action configuration. Gradle User Home was not restored from or saved to the cache.`

export const DEFAULT_WRITEONLY_REASON = `[Cache was set to write-only](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#using-the-cache-write-only) via action configuration. Gradle User Home was not restored from cache.`

export const EXISTING_GRADLE_HOME = `[Cache was disabled to avoid overwriting a pre-existing Gradle User Home](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#overwriting-an-existing-gradle-user-home). Gradle User Home was not restored from or saved to the cache.`

export const CLEANUP_DISABLED_READONLY = `[Cache cleanup](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#configuring-cache-cleanup) is always disabled when cache is read-only or disabled.`

export const DEFAULT_CLEANUP_ENABLED_REASON = `[Cache cleanup](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#configuring-cache-cleanup) was enabled. Stale files in Gradle User Home were purged before saving to the cache.`

export const DEFAULT_CLEANUP_DISABLED_REASON = `[Cache cleanup](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#configuring-cache-cleanup) was disabled via action parameter. No cleanup of Gradle User Home was performed.`

export const CLEANUP_DISABLED_DUE_TO_FAILURE =
    '[Cache cleanup was disabled due to build failure](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#configuring-cache-cleanup). Use `cache-cleanup: always` to override this behavior.'

export const CLEANUP_DISABLED_DUE_TO_CONFIG_CACHE_HIT =
    '[Cache cleanup was disabled due to configuration-cache reuse](https://github.com/gradle/actions/blob/main/docs/setup-gradle.md#configuring-cache-cleanup). This is expected.'

/**
 * Collects information on what entries were saved and restored during the action.
 * This information is used to generate a summary of the cache usage.
 */
export class CacheListener {
    cacheEntries: CacheEntryListener[] = []
    cacheReadOnly = false
    cacheWriteOnly = false
    cacheDisabled = false
    cacheStatusReason: string = DEFAULT_CACHE_ENABLED_REASON
    cacheCleanupMessage: string = DEFAULT_CLEANUP_DISABLED_REASON

    get fullyRestored(): boolean {
        return this.cacheEntries.every(x => !x.wasRequestedButNotRestored())
    }

    get cacheStatus(): string {
        if (!cache.isFeatureAvailable()) return 'not available'
        if (this.cacheDisabled) return 'disabled'
        if (this.cacheWriteOnly) return 'write-only'
        if (this.cacheReadOnly) return 'read-only'
        return 'enabled'
    }

    setReadOnly(reason: string = DEFAULT_READONLY_REASON): void {
        this.cacheReadOnly = true
        this.cacheStatusReason = reason
        this.cacheCleanupMessage = CLEANUP_DISABLED_READONLY
    }

    setDisabled(reason: string = DEFAULT_DISABLED_REASON): void {
        this.cacheDisabled = true
        this.cacheStatusReason = reason
        this.cacheCleanupMessage = CLEANUP_DISABLED_READONLY
    }

    setWriteOnly(reason: string = DEFAULT_WRITEONLY_REASON): void {
        this.cacheWriteOnly = true
        this.cacheStatusReason = reason
    }

    setCacheCleanupEnabled(): void {
        this.cacheCleanupMessage = DEFAULT_CLEANUP_ENABLED_REASON
    }

    setCacheCleanupDisabled(reason: string = DEFAULT_CLEANUP_DISABLED_REASON): void {
        this.cacheCleanupMessage = reason
    }

    entry(name: string): CacheEntryListener {
        for (const entry of this.cacheEntries) {
            if (entry.entryName === name) {
                return entry
            }
        }

        const newEntry = new CacheEntryListener(name)
        this.cacheEntries.push(newEntry)
        return newEntry
    }

    stringify(): string {
        return JSON.stringify(this)
    }

    static rehydrate(stringRep: string): CacheListener {
        if (stringRep === '') {
            return new CacheListener()
        }
        const rehydrated: CacheListener = Object.assign(new CacheListener(), JSON.parse(stringRep))
        const entries = rehydrated.cacheEntries
        for (let index = 0; index < entries.length; index++) {
            const rawEntry = entries[index]
            entries[index] = Object.assign(new CacheEntryListener(rawEntry.entryName), rawEntry)
        }
        return rehydrated
    }
}

/**
 * Collects information on the state of a single cache entry.
 */
export class CacheEntryListener {
    entryName: string
    requestedKey: string | undefined
    requestedRestoreKeys: string[] | undefined
    restoredKey: string | undefined
    restoredSize: number | undefined
    restoredTime: number | undefined
    notRestored: string | undefined

    savedKey: string | undefined
    savedSize: number | undefined
    savedTime: number | undefined
    notSaved: string | undefined

    constructor(entryName: string) {
        this.entryName = entryName
    }

    wasRequestedButNotRestored(): boolean {
        return this.requestedKey !== undefined && this.restoredKey === undefined
    }

    markRequested(key: string, restoreKeys: string[] = []): CacheEntryListener {
        this.requestedKey = key
        this.requestedRestoreKeys = restoreKeys
        return this
    }

    markRestored(key: string, size: number | undefined, time: number): CacheEntryListener {
        this.restoredKey = key
        this.restoredSize = size
        this.restoredTime = time
        return this
    }

    markNotRestored(message: string): CacheEntryListener {
        this.notRestored = message
        return this
    }

    markSaved(key: string, size: number | undefined, time: number): CacheEntryListener {
        this.savedKey = key
        this.savedSize = size
        this.savedTime = time
        return this
    }

    markAlreadyExists(key: string): CacheEntryListener {
        this.savedKey = key
        this.savedSize = 0
        return this
    }

    markNotSaved(message: string): CacheEntryListener {
        this.notSaved = message
        return this
    }
}

export function generateCachingReport(listener: CacheListener): string {
    const entries = listener.cacheEntries

    return `
<details>
<summary><h4>Caching for Gradle actions was ${listener.cacheStatus} - expand for details</h4></summary>

- ${listener.cacheStatusReason}
- ${listener.cacheCleanupMessage}

${renderEntryTable(entries)}

<h5>Cache Entry Details</h5>
<pre>
    ${renderEntryDetails(listener)}
</pre>
</details>
    `
}

function renderEntryTable(entries: CacheEntryListener[]): string {
    return `
<table>
    <tr><td></td><th>Count</th><th>Total Size (Mb)</th><th>Total Time (ms)</tr>
    <tr><td>Entries Restored</td>
        <td>${getCount(entries, e => e.restoredSize)}</td>
        <td>${getSize(entries, e => e.restoredSize)}</td>
        <td>${getTime(entries, e => e.restoredTime)}</td>
    </tr>
    <tr><td>Entries Saved</td>
        <td>${getCount(entries, e => e.savedSize)}</td>
        <td>${getSize(entries, e => e.savedSize)}</td>
        <td>${getTime(entries, e => e.savedTime)}</td>
    </tr>
</table>
    `
}

function renderEntryDetails(listener: CacheListener): string {
    return listener.cacheEntries
        .map(
            entry => `Entry: ${entry.entryName}
    Requested Key : ${entry.requestedKey ?? ''}
    Restored  Key : ${entry.restoredKey ?? ''}
              Size: ${formatSize(entry.restoredSize)}
              Time: ${formatTime(entry.restoredTime)}
              ${getRestoredMessage(entry, listener.cacheWriteOnly)}
    Saved     Key : ${entry.savedKey ?? ''}
              Size: ${formatSize(entry.savedSize)}
              Time: ${formatTime(entry.savedTime)}
              ${getSavedMessage(entry, listener.cacheReadOnly)}
`
        )
        .join('---\n')
}

function getRestoredMessage(entry: CacheEntryListener, cacheWriteOnly: boolean): string {
    if (entry.notRestored) {
        return `(Entry not restored: ${entry.notRestored})`
    }
    if (cacheWriteOnly) {
        return '(Entry not restored: cache is write-only)'
    }
    if (entry.requestedKey === undefined) {
        return '(Entry not restored: not requested)'
    }
    if (entry.restoredKey === undefined) {
        return '(Entry not restored: no match found)'
    }
    if (entry.restoredKey === entry.requestedKey) {
        return '(Entry restored: exact match found)'
    }
    return '(Entry restored: partial match found)'
}

function getSavedMessage(entry: CacheEntryListener, cacheReadOnly: boolean): string {
    if (entry.notSaved) {
        return `(Entry not saved: ${entry.notSaved})`
    }
    if (entry.savedKey === undefined) {
        if (cacheReadOnly) {
            return '(Entry not saved: cache is read-only)'
        }
        if (entry.notRestored) {
            return '(Entry not saved: not restored)'
        }
        return '(Entry not saved: reason unknown)'
    }
    if (entry.savedSize === 0) {
        return '(Entry not saved: entry with key already exists)'
    }
    return '(Entry saved)'
}

function getCount(
    cacheEntries: CacheEntryListener[],
    predicate: (value: CacheEntryListener) => number | undefined
): number {
    return cacheEntries.filter(e => predicate(e)).length
}

function getSize(
    cacheEntries: CacheEntryListener[],
    predicate: (value: CacheEntryListener) => number | undefined
): number {
    const bytes = cacheEntries.map(e => predicate(e) ?? 0).reduce((p, v) => p + v, 0)
    return Math.round(bytes / (1024 * 1024))
}

function getTime(
    cacheEntries: CacheEntryListener[],
    predicate: (value: CacheEntryListener) => number | undefined
): number {
    return cacheEntries.map(e => predicate(e) ?? 0).reduce((p, v) => p + v, 0)
}

function formatSize(bytes: number | undefined): string {
    if (bytes === undefined || bytes === 0) {
        return ''
    }
    return `${Math.round(bytes / (1024 * 1024))} MB (${bytes} B)`
}

function formatTime(ms: number | undefined): string {
    if (ms === undefined || ms === 0) {
        return ''
    }
    return `${ms} ms`
}
