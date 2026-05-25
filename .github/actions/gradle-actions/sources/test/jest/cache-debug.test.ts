import * as path from 'path'
import * as fs from 'fs'
import {describe, expect, it} from '@jest/globals'

import {GradleUserHomeCache} from "../../src/caching/gradle-user-home-cache"
import {CacheConfig} from "../../src/configuration"

const testTmp = 'test/jest/tmp'
fs.rmSync(testTmp, {recursive: true, force: true})

describe("--info and --stacktrace", () => {
    describe("will be created", () => {
        it("when gradle.properties does not exist", async () => {
            const emptyGradleHome = `${testTmp}/empty-gradle-home`
            fs.mkdirSync(emptyGradleHome, {recursive: true})

            const stateCache = new GradleUserHomeCache("ignored", emptyGradleHome, new CacheConfig())
            stateCache.configureInfoLogLevel()

            expect(fs.readFileSync(path.resolve(emptyGradleHome, "gradle.properties"), 'utf-8'))
                .toBe("org.gradle.logging.level=info\norg.gradle.logging.stacktrace=all\n")
        })
    })
    describe("will be added", () => {
        it("and gradle.properties does exists", async () => {
            const existingGradleHome = `${testTmp}/existing-gradle-home`
            fs.mkdirSync(existingGradleHome, {recursive: true})
            fs.writeFileSync(path.resolve(existingGradleHome, "gradle.properties"), "org.gradle.logging.level=debug\n")

            const stateCache = new GradleUserHomeCache("ignored", existingGradleHome, new CacheConfig())
            stateCache.configureInfoLogLevel()

            expect(fs.readFileSync(path.resolve(existingGradleHome, "gradle.properties"), 'utf-8'))
                .toBe("org.gradle.logging.level=info\norg.gradle.logging.stacktrace=all\n\norg.gradle.logging.level=debug\n")
        })
    })
})
