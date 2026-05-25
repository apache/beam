import fs from 'fs'
import path from 'path'
import {ACTION_METADATA_DIR} from '../configuration'
import * as core from '@actions/core'

export class ChecksumCache {
    private readonly cacheFile: string

    constructor(gradleUserHome: string) {
        this.cacheFile = path.resolve(gradleUserHome, ACTION_METADATA_DIR, 'valid-wrappers.json')
    }

    load(): string[] {
        // Load previously validated checksums saved in Gradle User Home
        if (fs.existsSync(this.cacheFile)) {
            try {
                return JSON.parse(fs.readFileSync(this.cacheFile, 'utf-8'))
            } catch (e) {
                core.warning(`Failed to parse checksum cache file: ${e}`)
            }
        }
        return []
    }

    save(checksums: string[]): void {
        const uniqueChecksums = [...new Set(checksums)]
        // Save validated checksums to Gradle User Home
        fs.mkdirSync(path.dirname(this.cacheFile), {recursive: true})
        fs.writeFileSync(this.cacheFile, JSON.stringify(uniqueChecksums))
    }
}
