import dedent from 'dedent'
import {describe, expect, it} from '@jest/globals'

import {BuildResult} from '../../src/build-results'
import {renderSummaryTable} from '../../src/job-summary'

const successfulHelpBuild: BuildResult = {
    rootProjectName: 'root',
    rootProjectDir: '/',
    requestedTasks: 'help',
    gradleVersion: '8.0',
    gradleHomeDir: '/opt/gradle',
    buildFailed: false,
    configCacheHit: false,
    buildScanUri: 'https://scans.gradle.com/s/abc123',
    buildScanFailed: false
}

const failedHelpBuild: BuildResult = {
    ...successfulHelpBuild,
    buildFailed: true
}

const longArgsBuild: BuildResult = {
    ...successfulHelpBuild,
    requestedTasks: 'check publishMyLongNamePluginPublicationToMavenCentral publishMyLongNamePluginPublicationToPluginPortal',
}

const scanPublishDisabledBuild: BuildResult = {
    ...successfulHelpBuild,
    buildScanUri: '',
    buildScanFailed: false,
}

const scanPublishFailedBuild: BuildResult = {
    ...successfulHelpBuild,
    buildScanUri: '',
    buildScanFailed: true,
}

describe('renderSummaryTable', () => {
    describe('renders', () => {
        it('successful build', () => {
            const table = renderSummaryTable([successfulHelpBuild])
            expect(table.trim()).toBe(dedent`
                <table>
                    <tr>
                        <th>Gradle Root Project</th>
                        <th>Requested Tasks</th>
                        <th>Gradle Version</th>
                        <th>Build Outcome</th>
                        <th>Build&nbsp;Scan®</th>
                    </tr>
                    <tr>
                        <td>root</td>
                        <td>help</td>
                        <td align='center'>8.0</td>
                        <td align='center'>:white_check_mark:</td>
                        <td><a href="https://scans.gradle.com/s/abc123" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Build%20Scan%C2%AE-06A0CE?logo=Gradle" alt="Build Scan published" /></a></td>
                    </tr>
                </table>
            `);
        })
        it('failed build', () => {
            const table = renderSummaryTable([failedHelpBuild])
            expect(table.trim()).toBe(dedent`
                <table>
                    <tr>
                        <th>Gradle Root Project</th>
                        <th>Requested Tasks</th>
                        <th>Gradle Version</th>
                        <th>Build Outcome</th>
                        <th>Build&nbsp;Scan®</th>
                    </tr>
                    <tr>
                        <td>root</td>
                        <td>help</td>
                        <td align='center'>8.0</td>
                        <td align='center'>:x:</td>
                        <td><a href="https://scans.gradle.com/s/abc123" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Build%20Scan%C2%AE-06A0CE?logo=Gradle" alt="Build Scan published" /></a></td>
                    </tr>
                </table>
            `);
        })
        describe('when build scan', () => {
            it('publishing disabled', () => {
                const table = renderSummaryTable([scanPublishDisabledBuild])
                expect(table.trim()).toBe(dedent`
                    <table>
                        <tr>
                            <th>Gradle Root Project</th>
                            <th>Requested Tasks</th>
                            <th>Gradle Version</th>
                            <th>Build Outcome</th>
                            <th>Build&nbsp;Scan®</th>
                        </tr>
                        <tr>
                            <td>root</td>
                            <td>help</td>
                            <td align='center'>8.0</td>
                            <td align='center'>:white_check_mark:</td>
                            <td><a href="https://scans.gradle.com" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Not%20published-lightgrey" alt="Build Scan not published" /></a></td>
                        </tr>
                    </table>
                `);
            })
            it('publishing failed', () => {
                const table = renderSummaryTable([scanPublishFailedBuild])
                expect(table.trim()).toBe(dedent`
                    <table>
                        <tr>
                            <th>Gradle Root Project</th>
                            <th>Requested Tasks</th>
                            <th>Gradle Version</th>
                            <th>Build Outcome</th>
                            <th>Build&nbsp;Scan®</th>
                        </tr>
                        <tr>
                            <td>root</td>
                            <td>help</td>
                            <td align='center'>8.0</td>
                            <td align='center'>:white_check_mark:</td>
                            <td><a href="https://docs.gradle.com/develocity/gradle-plugin/#troubleshooting" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Publish%20failed-orange" alt="Build Scan publish failed" /></a></td>
                        </tr>
                    </table>
                `);
            })
        })
        it('multiple builds', () => {
            const table = renderSummaryTable([successfulHelpBuild, failedHelpBuild])
            expect(table.trim()).toBe(dedent`
                <table>
                    <tr>
                        <th>Gradle Root Project</th>
                        <th>Requested Tasks</th>
                        <th>Gradle Version</th>
                        <th>Build Outcome</th>
                        <th>Build&nbsp;Scan®</th>
                    </tr>
                    <tr>
                        <td>root</td>
                        <td>help</td>
                        <td align='center'>8.0</td>
                        <td align='center'>:white_check_mark:</td>
                        <td><a href="https://scans.gradle.com/s/abc123" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Build%20Scan%C2%AE-06A0CE?logo=Gradle" alt="Build Scan published" /></a></td>
                    </tr>
                    <tr>
                        <td>root</td>
                        <td>help</td>
                        <td align='center'>8.0</td>
                        <td align='center'>:x:</td>
                        <td><a href="https://scans.gradle.com/s/abc123" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Build%20Scan%C2%AE-06A0CE?logo=Gradle" alt="Build Scan published" /></a></td>
                    </tr>
                </table>
            `);
        })
        it('truncating long requested tasks', () => {
            const table = renderSummaryTable([longArgsBuild])
            expect(table.trim()).toBe(dedent`
                <table>
                    <tr>
                        <th>Gradle Root Project</th>
                        <th>Requested Tasks</th>
                        <th>Gradle Version</th>
                        <th>Build Outcome</th>
                        <th>Build&nbsp;Scan®</th>
                    </tr>
                    <tr>
                        <td>root</td>
                        <td><div title='check publishMyLongNamePluginPublicationToMavenCentral publishMyLongNamePluginPublicationToPluginPortal'>check publishMyLongNamePluginPublicationToMavenCentral publ…</div></td>
                        <td align='center'>8.0</td>
                        <td align='center'>:white_check_mark:</td>
                        <td><a href="https://scans.gradle.com/s/abc123" rel="nofollow" target="_blank"><img src="https://img.shields.io/badge/Build%20Scan%C2%AE-06A0CE?logo=Gradle" alt="Build Scan published" /></a></td>
                    </tr>
                </table>
            `);
        })
    })
})
