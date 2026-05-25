import * as core from '@actions/core'
import * as github from '@actions/github'
import * as glob from '@actions/glob'
import {DefaultArtifactClient} from '@actions/artifact'
import {GitHub} from '@actions/github/lib/utils'
import type {PullRequestEvent} from '@octokit/webhooks-types'

import * as path from 'path'
import fs from 'fs'

import {JobFailure} from './errors'
import {DependencyGraphConfig, DependencyGraphOption, getGithubToken, getWorkspaceDirectory} from './configuration'

const DEPENDENCY_GRAPH_PREFIX = 'dependency-graph_'

export async function setup(config: DependencyGraphConfig): Promise<void> {
    const option = config.getDependencyGraphOption()
    if (option === DependencyGraphOption.Disabled) {
        core.exportVariable('GITHUB_DEPENDENCY_GRAPH_ENABLED', 'false')
        return
    }
    // Download and submit early, for compatability with dependency review.
    if (option === DependencyGraphOption.DownloadAndSubmit) {
        maybeExportVariable('DEPENDENCY_GRAPH_REPORT_DIR', config.getReportDirectory())
        await downloadAndSubmitDependencyGraphs(config)
        return
    }

    core.info('Enabling dependency graph generation')
    core.exportVariable('GITHUB_DEPENDENCY_GRAPH_ENABLED', 'true')
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_CONTINUE_ON_FAILURE', config.getDependencyGraphContinueOnFailure())
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_JOB_CORRELATOR', config.getJobCorrelator())
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_JOB_ID', github.context.runId.toString())
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_REF', github.context.ref)
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_SHA', getShaFromContext())
    maybeExportVariable('GITHUB_DEPENDENCY_GRAPH_WORKSPACE', getWorkspaceDirectory())
    maybeExportVariable('DEPENDENCY_GRAPH_REPORT_DIR', config.getReportDirectory())

    maybeExportVariable('DEPENDENCY_GRAPH_EXCLUDE_PROJECTS', config.getExcludeProjects())
    maybeExportVariable('DEPENDENCY_GRAPH_INCLUDE_PROJECTS', config.getIncludeProjects())
    maybeExportVariable('DEPENDENCY_GRAPH_EXCLUDE_CONFIGURATIONS', config.getExcludeConfigurations())
    maybeExportVariable('DEPENDENCY_GRAPH_INCLUDE_CONFIGURATIONS', config.getIncludeConfigurations())

    maybeExportVariable('GRADLE_PLUGIN_REPOSITORY_URL', config.getPluginRepository().getUrl())
    maybeExportVariable('GRADLE_PLUGIN_REPOSITORY_USERNAME', config.getPluginRepository().getUsername())
    maybeExportVariable('GRADLE_PLUGIN_REPOSITORY_PASSWORD', config.getPluginRepository().getPassword())
}

function maybeExportVariable(variableName: string, value: string | boolean | undefined): void {
    if (!process.env[variableName]) {
        if (value !== undefined) {
            core.exportVariable(variableName, value)
        }
    }
}

export async function complete(config: DependencyGraphConfig): Promise<void> {
    const option = config.getDependencyGraphOption()
    try {
        switch (option) {
            case DependencyGraphOption.Disabled:
            case DependencyGraphOption.Generate: // Performed via init-script: nothing to do here
            case DependencyGraphOption.DownloadAndSubmit: // Performed in setup
                return
            case DependencyGraphOption.GenerateAndSubmit:
                await findAndSubmitDependencyGraphs(config, false)
                return
            case DependencyGraphOption.GenerateSubmitAndUpload:
                await findAndSubmitDependencyGraphs(config, true)
                return
            case DependencyGraphOption.GenerateAndUpload:
                await findAndUploadDependencyGraphs(config)
        }
    } catch (e) {
        warnOrFail(config, option, e)
    }
}

async function downloadAndSubmitDependencyGraphs(config: DependencyGraphConfig): Promise<void> {
    if (isRunningInActEnvironment()) {
        core.info('Dependency graph not supported in the ACT environment.')
        return
    }

    try {
        await submitDependencyGraphs(await downloadDependencyGraphs(config))
    } catch (e) {
        warnOrFail(config, DependencyGraphOption.DownloadAndSubmit, e)
    }
}

async function findAndSubmitDependencyGraphs(config: DependencyGraphConfig, uploadAfterSubmit: boolean): Promise<void> {
    if (isRunningInActEnvironment()) {
        core.info('Dependency graph not supported in the ACT environment.')
        return
    }

    const dependencyGraphFiles = await findDependencyGraphFiles()
    try {
        await submitDependencyGraphs(dependencyGraphFiles)
    } catch (e) {
        try {
            await uploadDependencyGraphs(dependencyGraphFiles, config)
        } catch (uploadError) {
            core.info(String(uploadError))
        }
        throw e
    }

    if (uploadAfterSubmit) {
        await uploadDependencyGraphs(dependencyGraphFiles, config)
    }
}

async function findAndUploadDependencyGraphs(config: DependencyGraphConfig): Promise<void> {
    if (isRunningInActEnvironment()) {
        core.info('Dependency graph not supported in the ACT environment.')
        return
    }

    await uploadDependencyGraphs(await findDependencyGraphFiles(), config)
}

async function downloadDependencyGraphs(config: DependencyGraphConfig): Promise<string[]> {
    const findBy = github.context.payload.workflow_run
        ? {
              token: getGithubToken(),
              workflowRunId: github.context.payload.workflow_run.id,
              repositoryName: github.context.repo.repo,
              repositoryOwner: github.context.repo.owner
          }
        : undefined

    const artifactClient = new DefaultArtifactClient()

    let dependencyGraphArtifacts = (
        await artifactClient.listArtifacts({
            latest: true,
            findBy
        })
    ).artifacts.filter(artifact => artifact.name.startsWith(DEPENDENCY_GRAPH_PREFIX))

    const artifactName = config.getDownloadArtifactName()
    if (artifactName) {
        core.info(`Filtering for artifacts ending with ${artifactName}`)
        dependencyGraphArtifacts = dependencyGraphArtifacts.filter(artifact => artifact.name.includes(artifactName))
    }

    for (const artifact of dependencyGraphArtifacts) {
        const downloadedArtifact = await artifactClient.downloadArtifact(artifact.id, {
            findBy
        })
        core.info(`Downloading dependency-graph artifact ${artifact.name} to ${downloadedArtifact.downloadPath}`)
    }

    return findDependencyGraphFiles()
}

async function findDependencyGraphFiles(): Promise<string[]> {
    const globber = await glob.create(`${getReportDirectory()}/**/*.json`)
    const allFiles = await globber.glob()
    const unprocessedFiles = allFiles.filter(file => !isProcessed(file))
    unprocessedFiles.forEach(markProcessed)
    core.info(`Found dependency graph files: ${unprocessedFiles.join(', ')}`)
    return unprocessedFiles
}

async function uploadDependencyGraphs(dependencyGraphFiles: string[], config: DependencyGraphConfig): Promise<void> {
    if (dependencyGraphFiles.length === 0) {
        core.info('No dependency graph files found to upload.')
        return
    }

    const workspaceDirectory = getWorkspaceDirectory()

    const artifactClient = new DefaultArtifactClient()
    for (const dependencyGraphFile of dependencyGraphFiles) {
        const relativePath = getRelativePathFromWorkspace(dependencyGraphFile)
        core.info(`Uploading dependency graph file: ${relativePath}`)
        const artifactName = `${DEPENDENCY_GRAPH_PREFIX}${path.basename(dependencyGraphFile)}`
        await artifactClient.uploadArtifact(artifactName, [dependencyGraphFile], workspaceDirectory, {
            retentionDays: config.getArtifactRetentionDays()
        })
    }
}

async function submitDependencyGraphs(dependencyGraphFiles: string[]): Promise<void> {
    if (dependencyGraphFiles.length === 0) {
        core.info('No dependency graph files found to submit.')
        return
    }

    for (const dependencyGraphFile of dependencyGraphFiles) {
        try {
            await submitDependencyGraphFile(dependencyGraphFile)
        } catch (error) {
            if (error instanceof Error && error.name === 'HttpError') {
                error.message = translateErrorMessage(dependencyGraphFile, error)
            }
            throw error
        }
    }
}

function translateErrorMessage(jsonFile: string, error: Error): string {
    const relativeJsonFile = getRelativePathFromWorkspace(jsonFile)
    const mainWarning = `Dependency submission failed for ${relativeJsonFile}.\n${error.message}`
    if (error.message === 'Resource not accessible by integration') {
        return `${mainWarning}
Please ensure that the 'contents: write' permission is available for the workflow job.
Note that this permission is never available for a 'pull_request' trigger from a repository fork.
        `
    }
    return mainWarning
}

async function submitDependencyGraphFile(jsonFile: string): Promise<void> {
    const octokit = getOctokit()
    const jsonContent = fs.readFileSync(jsonFile, 'utf8')

    const jsonObject = JSON.parse(jsonContent)
    jsonObject.owner = github.context.repo.owner
    jsonObject.repo = github.context.repo.repo
    const response = await octokit.request('POST /repos/{owner}/{repo}/dependency-graph/snapshots', jsonObject)

    const relativeJsonFile = getRelativePathFromWorkspace(jsonFile)
    core.notice(`Submitted ${relativeJsonFile}: ${response.data.message}`)
}
function getReportDirectory(): string {
    return process.env.DEPENDENCY_GRAPH_REPORT_DIR!
}

function isProcessed(dependencyGraphFile: string): boolean {
    const markerFile = `${dependencyGraphFile}.processed`
    return fs.existsSync(markerFile)
}

function markProcessed(dependencyGraphFile: string): void {
    const markerFile = `${dependencyGraphFile}.processed`
    fs.writeFileSync(markerFile, '')
}

function warnOrFail(config: DependencyGraphConfig, option: String, error: unknown): void {
    if (!config.getDependencyGraphContinueOnFailure()) {
        throw new JobFailure(error)
    }

    core.warning(`Failed to ${option} dependency graph. Will continue.\n${String(error)}`)
}

function getOctokit(): InstanceType<typeof GitHub> {
    return github.getOctokit(getGithubToken())
}

function getRelativePathFromWorkspace(file: string): string {
    const workspaceDirectory = getWorkspaceDirectory()
    return path.relative(workspaceDirectory, file)
}

function getShaFromContext(): string {
    const context = github.context
    const pullRequestEvents = [
        'pull_request',
        'pull_request_comment',
        'pull_request_review',
        'pull_request_review_comment'
        // Note that pull_request_target is omitted here.
        // That event runs in the context of the base commit of the PR,
        // so the snapshot should not be associated with the head commit.
    ]
    if (pullRequestEvents.includes(context.eventName)) {
        const pr = (context.payload as PullRequestEvent).pull_request
        return pr.head.sha
    } else {
        return context.sha
    }
}

function isRunningInActEnvironment(): boolean {
    return process.env.ACT !== undefined
}
