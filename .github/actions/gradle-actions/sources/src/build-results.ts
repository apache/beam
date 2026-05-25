import * as fs from 'fs'
import * as path from 'path'
import {versionIsAtLeast} from './execution/gradle'

export interface BuildResult {
    get rootProjectName(): string
    get rootProjectDir(): string
    get requestedTasks(): string
    get gradleVersion(): string
    get gradleHomeDir(): string
    get buildFailed(): boolean
    get configCacheHit(): boolean
    get buildScanUri(): string
    get buildScanFailed(): boolean
}

export class BuildResults {
    results: BuildResult[]

    constructor(results: BuildResult[]) {
        this.results = results
    }

    anyFailed(): boolean {
        return this.results.some(result => result.buildFailed)
    }

    anyConfigCacheHit(): boolean {
        return this.results.some(result => result.configCacheHit)
    }

    uniqueGradleHomes(): string[] {
        const allHomes = this.results.map(buildResult => buildResult.gradleHomeDir)
        return Array.from(new Set(allHomes))
    }

    highestGradleVersion(): string | null {
        if (this.results.length === 0) {
            return null
        }
        return this.results
            .map(result => result.gradleVersion)
            .reduce((maxVersion: string, currentVersion: string) => {
                if (!maxVersion) return currentVersion
                return versionIsAtLeast(currentVersion, maxVersion) ? currentVersion : maxVersion
            })
    }
}

export function loadBuildResults(): BuildResults {
    const results = getUnprocessedResults().map(filePath => {
        const content = fs.readFileSync(filePath, 'utf8')
        const buildResult = JSON.parse(content) as BuildResult
        addScanResults(filePath, buildResult)
        return buildResult
    })
    return new BuildResults(results)
}

export function markBuildResultsProcessed(): void {
    getUnprocessedResults().forEach(markProcessed)
}

function getUnprocessedResults(): string[] {
    const buildResultsDir = path.resolve(process.env['RUNNER_TEMP']!, '.gradle-actions', 'build-results')
    if (!fs.existsSync(buildResultsDir)) {
        return []
    }

    return fs
        .readdirSync(buildResultsDir)
        .map(file => {
            return path.resolve(buildResultsDir, file)
        })
        .filter(filePath => {
            return path.extname(filePath) === '.json' && !isProcessed(filePath)
        })
}

function addScanResults(buildResultsFile: string, buildResult: BuildResult): void {
    const buildScansDir = path.resolve(process.env['RUNNER_TEMP']!, '.gradle-actions', 'build-scans')
    if (!fs.existsSync(buildScansDir)) {
        return
    }

    const buildScanResults = path.resolve(buildScansDir, path.basename(buildResultsFile))
    if (fs.existsSync(buildScanResults)) {
        const content = fs.readFileSync(buildScanResults, 'utf8')
        const scanResults = JSON.parse(content)
        Object.assign(buildResult, scanResults)
    }

    return
}

function isProcessed(resultFile: string): boolean {
    const markerFile = `${resultFile}.processed`
    return fs.existsSync(markerFile)
}

function markProcessed(resultFile: string): void {
    const markerFile = `${resultFile}.processed`
    fs.writeFileSync(markerFile, '')
}
