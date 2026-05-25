import {describe, expect, it} from '@jest/globals'

import {versionIsAtLeast, parseGradleVersionFromOutput} from '../../src/execution/gradle'

describe('gradle', () => {
    describe('can compare versions that are', () => {
        function versionsAreOrdered(versions: string[]): void {
            for (let i = 0; i < versions.length; i++) {
                // Compare with all other versions
                for (let j = 0; j < versions.length; j++) {
                    if (i >= j) {
                        it(`${versions[i]} is at least ${versions[j]}`, () => {
                            expect(versionIsAtLeast(versions[i], versions[j])).toBe(true)
                        })
                    } else {
                        it(`${versions[i]} is NOT at least ${versions[j]}`, () => {
                            expect(versionIsAtLeast(versions[i], versions[j])).toBe(false)
                        })
                    }
                }
            }
        }

        function versionsAreNotOrdered(versions: string[]): void {
            for (let i = 0; i < versions.length; i++) {
                // Compare with all other versions
                for (let j = 0; j < versions.length; j++) {
                    if (i !== j) {
                        it(`${versions[i]} is NOT at least ${versions[j]}`, () => {
                            expect(versionIsAtLeast(versions[i], versions[j])).toBe(false)
                        })
                    }
                }
            }
        }

        function versionsAreEqual(versions: string[]): void {
            for (let i = 0; i < versions.length; i++) {
                // Compare with all other versions
                for (let j = 0; j < versions.length; j++) {
                    it(`${versions[i]} is at least ${versions[j]}`, () => {
                        expect(versionIsAtLeast(versions[i], versions[j])).toBe(true)
                    })
                }
            }
        }

        describe('simple versions', () => {
            versionsAreOrdered(['6.0', '6.7', '6.7.1', '6.7.2', '7.0', '7.0.1', '7.1', '8.0', '8.12.1'])

            versionsAreEqual(['7.0', '7.0.0'])
            versionsAreEqual(['7.1', '7.1.0'])
        })

        describe('rc versions', () => {
            versionsAreOrdered([
                '8.10', '8.11-rc-1', '8.11-rc-2', '8.11', '8.11.1-rc-1', '8.11.1'
            ])
        })

        describe('milestone versions', () => {
            versionsAreOrdered([
                '8.12.1', '8.12.2-milestone-1', '8.12.2', '8.13-milestone-1', '8.13-milestone-2', '8.13'
            ])
            versionsAreOrdered([
                '8.12.1', '8.12.2-milestone-1', '8.12.2-milestone-2', '8.12.2-rc-1', '8.12.2'
            ])
        })

        describe('preview versions', () => {
            versionsAreOrdered([
                '8.12.1', '8.12.2-preview-1', '8.12.2', '8.13-preview-1', '8.13-preview-2', '8.13'
            ])
            versionsAreOrdered([
                '8.12.1', '8.12.2-milestone-1', '8.12.2-preview-1', '8.12.2-rc-1', '8.12.2'
            ])
        })

        describe('snapshot versions', () => {
            versionsAreOrdered([
                '8.10.1', '8.10.2-20240828012138+0000', '8.10.2', '8.11-20240829002031+0000', '8.11'
            ])
            versionsAreOrdered([
                '9.0', '9.1-branch-provider_api_migration_public_api_changes-20240826121451+0000', '9.1'
            ])
            versionsAreNotOrdered([
                '8.10.2-20240828012138+0000', '8.10.2-20240828010000+1000', '8.10.2-milestone-1'
            ])
        })
    })

    describe('can parse version from output', () => {
        it('major version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 8.9
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('8.9')
        })
    
        it('patch version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 8.9.1
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('8.9.1')
        })
    
        it('rc version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 8.9-rc-1
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('8.9-rc-1')
        })
    
        it('milestone version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 8.0-milestone-6
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('8.0-milestone-6')
        })
    
        it('snapshot version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 8.10.2-20240828012138+0000
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('8.10.2-20240828012138+0000')
        })
    
        it('branch version', async () => {
            const output = `
    ------------------------------------------------------------
    Gradle 9.0-branch-provider_api_migration_public_api_changes-20240830060514+0000
    ------------------------------------------------------------
    `
            const version = await parseGradleVersionFromOutput(output)!
            expect(version).toBe('9.0-branch-provider_api_migration_public_api_changes-20240830060514+0000')
        })
    })
})

