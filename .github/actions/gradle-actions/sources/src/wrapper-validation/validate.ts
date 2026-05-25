import * as find from './find'
import * as checksums from './checksums'
import * as hash from './hash'
import {resolve} from 'path'

export async function findInvalidWrapperJars(
    gitRepoRoot: string,
    allowSnapshots: boolean,
    allowedChecksums: string[],
    previouslyValidatedChecksums: string[] = [],
    knownValidChecksums: checksums.WrapperChecksums = checksums.KNOWN_CHECKSUMS
): Promise<ValidationResult> {
    const wrapperJars = await find.findWrapperJars(gitRepoRoot)
    const result = new ValidationResult([], [])
    if (wrapperJars.length > 0) {
        const notYetValidatedWrappers = []
        for (const wrapperJar of wrapperJars) {
            const sha = await hash.sha256File(resolve(gitRepoRoot, wrapperJar))
            if (
                allowedChecksums.includes(sha) ||
                previouslyValidatedChecksums.includes(sha) ||
                knownValidChecksums.checksums.has(sha)
            ) {
                result.valid.push(new WrapperJar(wrapperJar, sha))
            } else {
                notYetValidatedWrappers.push(new WrapperJar(wrapperJar, sha))
            }
        }

        // Otherwise fall back to fetching checksums from Gradle API and compare against them
        if (notYetValidatedWrappers.length > 0) {
            result.fetchedChecksums = true
            const fetchedValidChecksums = await checksums.fetchUnknownChecksums(allowSnapshots, knownValidChecksums)

            for (const wrapperJar of notYetValidatedWrappers) {
                if (!fetchedValidChecksums.checksums.has(wrapperJar.checksum)) {
                    result.invalid.push(wrapperJar)
                } else {
                    result.valid.push(wrapperJar)
                }
            }
        }
    }
    return result
}

export class ValidationResult {
    valid: WrapperJar[]
    invalid: WrapperJar[]
    fetchedChecksums = false

    constructor(valid: WrapperJar[], invalid: WrapperJar[]) {
        this.valid = valid
        this.invalid = invalid
    }

    isValid(): boolean {
        return this.invalid.length === 0
    }

    toDisplayString(): string {
        let displayString = ''
        if (this.invalid.length > 0) {
            displayString += `✗ Found unknown Gradle Wrapper JAR files:\n${ValidationResult.toDisplayList(
                this.invalid
            )}`
        }
        if (this.valid.length > 0) {
            if (displayString.length > 0) displayString += '\n'
            displayString += `✓ Found known Gradle Wrapper JAR files:\n${ValidationResult.toDisplayList(this.valid)}`
        }
        return displayString
    }

    private static toDisplayList(wrapperJars: WrapperJar[]): string {
        return `  ${wrapperJars.map(wj => wj.toDisplayString()).join(`\n  `)}`
    }
}

export class WrapperJar {
    path: string
    checksum: string

    constructor(path: string, checksum: string) {
        this.path = path
        this.checksum = checksum
    }

    toDisplayString(): string {
        return `${this.checksum} ${this.path}`
    }
}
