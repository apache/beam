import * as core from '@actions/core'
import * as github from '@actions/github'

import {BuildResults, BuildResult} from './build-results'
import {SummaryConfig, getActionId, getGithubToken} from './configuration'
import {Deprecation, getDeprecations, getErrors} from './deprecation-collector'

export async function generateJobSummary(
    buildResults: BuildResults,
    cachingReport: string,
    config: SummaryConfig
): Promise<void> {
    const errors = renderErrors()
    if (errors) {
        core.summary.addRaw(errors)
        await core.summary.write()
        return
    }

    const summaryTable = renderSummaryTable(buildResults.results)

    const hasFailure = buildResults.anyFailed()
    if (config.shouldGenerateJobSummary(hasFailure)) {
        core.info('Generating Job Summary')

        core.summary.addRaw(summaryTable)
        core.summary.addRaw(cachingReport)
        await core.summary.write()
    } else {
        core.info('============================')
        core.info(summaryTable)
        core.info('============================')
        core.info(cachingReport)
        core.info('============================')
    }

    if (config.shouldAddPRComment(hasFailure)) {
        await addPRComment(summaryTable)
    }
}

async function addPRComment(jobSummary: string): Promise<void> {
    const context = github.context
    if (context.payload.pull_request == null) {
        core.info('No pull_request trigger detected: not adding PR comment')
        return
    }

    const pull_request_number = context.payload.pull_request.number
    core.info(`Adding Job Summary as comment to PR #${pull_request_number}.`)

    const prComment = `<h3>Job Summary for Gradle</h3>
<a href="${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}" target="_blank">
<h5>${context.workflow} :: <em>${context.job}</em></h5>
</a>

${jobSummary}`

    const github_token = getGithubToken()
    const octokit = github.getOctokit(github_token)
    try {
        await octokit.rest.issues.createComment({
            ...context.repo,
            issue_number: pull_request_number,
            body: prComment
        })
    } catch (error) {
        if (error instanceof Error && error.name === 'HttpError') {
            core.warning(buildWarningMessage(error))
        } else {
            throw error
        }
    }
}

function buildWarningMessage(error: Error): string {
    const mainWarning = `Failed to generate PR comment.\n${String(error)}`
    if (error.message === 'Resource not accessible by integration') {
        return `${mainWarning}
Please ensure that the 'pull-requests: write' permission is available for the workflow job.
Note that this permission is never available for a workflow triggered from a repository fork.
`
    }
    return mainWarning
}

export function renderSummaryTable(results: BuildResult[]): string {
    return `${renderDeprecations()}\n${renderBuildResults(results)}`
}

function renderErrors(): string | undefined {
    const errors = getErrors()
    if (errors.length === 0) {
        return undefined
    }
    return errors.map(error => `<b>:x: ${error}</b>`).join('\n')
}

function renderDeprecations(): string {
    const deprecations = getDeprecations()
    if (deprecations.length === 0) {
        return ''
    }
    return `
<h4>Deprecation warnings</h4>
This job uses deprecated functionality from the <code>${getActionId()}</code> action. Follow the links for upgrade details.
<ul>
    ${deprecations.map(deprecation => `<li>${getDeprecationHtml(deprecation)}</li>`).join('')}
</ul>

<h4>Gradle Build Results</h4>`
}

function getDeprecationHtml(deprecation: Deprecation): string {
    return `<a href="${deprecation.getDocumentationLink()}" target="_blank">${deprecation.message}</a>`
}

function renderBuildResults(results: BuildResult[]): string {
    if (results.length === 0) {
        return '<b>No Gradle build results detected.</b>'
    }

    return `
<table>
    <tr>
        <th>Gradle Root Project</th>
        <th>Requested Tasks</th>
        <th>Gradle Version</th>
        <th>Build Outcome</th>
        <th>Build&nbsp;Scan®</th>
    </tr>${results.map(result => renderBuildResultRow(result)).join('')}
</table>
    `
}

function renderBuildResultRow(result: BuildResult): string {
    return `
    <tr>
        <td>${truncateString(result.rootProjectName, 30)}</td>
        <td>${truncateString(result.requestedTasks, 60)}</td>
        <td align='center'>${result.gradleVersion}</td>
        <td align='center'>${renderOutcome(result)}</td>
        <td>${renderBuildScan(result)}</td>
    </tr>`
}

function renderOutcome(result: BuildResult): string {
    return result.buildFailed ? ':x:' : ':white_check_mark:'
}

interface BadgeSpec {
    text: string
    alt: string
    color: string
    logo: boolean
    targetUrl: string
}

function renderBuildScan(result: BuildResult): string {
    if (result.buildScanFailed) {
        return renderBuildScanBadge({
            text: 'Publish failed',
            alt: 'Build Scan publish failed',
            color: 'orange',
            logo: false,
            targetUrl: 'https://docs.gradle.com/develocity/gradle-plugin/#troubleshooting'
        })
    }
    if (result.buildScanUri) {
        return renderBuildScanBadge({
            text: 'Build Scan®',
            alt: 'Build Scan published',
            color: '06A0CE',
            logo: true,
            targetUrl: result.buildScanUri
        })
    }
    return renderBuildScanBadge({
        text: 'Not published',
        alt: 'Build Scan not published',
        color: 'lightgrey',
        logo: false,
        targetUrl: 'https://scans.gradle.com'
    })
}

function renderBuildScanBadge({text, alt, color, logo, targetUrl}: BadgeSpec): string {
    const encodedText = encodeURIComponent(text)
    const badgeUrl = `https://img.shields.io/badge/${encodedText}-${color}${logo ? '?logo=Gradle' : ''}`
    const badgeHtml = `<img src="${badgeUrl}" alt="${alt}" />`
    return `<a href="${targetUrl}" rel="nofollow" target="_blank">${badgeHtml}</a>`
}

function truncateString(str: string, maxLength: number): string {
    if (str.length > maxLength) {
        return `<div title='${str}'>${str.slice(0, maxLength - 1)}…</div>`
    } else {
        return str
    }
}
