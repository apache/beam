import * as core from '@actions/core'
import {getActionId} from './configuration'

const DEPRECATION_UPGRADE_PAGE = 'https://github.com/gradle/actions/blob/main/docs/deprecation-upgrade-guide.md'
const recordedDeprecations: Deprecation[] = []
const recordedErrors: string[] = []

export class Deprecation {
    constructor(readonly message: string) {}

    getDocumentationLink(): string {
        const deprecationAnchor = this.message
            .toLowerCase()
            .replace(/[^\w\s-]|_/g, '')
            .replace(/ /g, '-')
        return `${DEPRECATION_UPGRADE_PAGE}#${deprecationAnchor}`
    }
}

export function recordDeprecation(message: string): void {
    if (!recordedDeprecations.some(deprecation => deprecation.message === message)) {
        recordedDeprecations.push(new Deprecation(message))
    }
}

export function failOnUseOfRemovedFeature(removalMessage: string, deprecationMessage: string = removalMessage): void {
    const deprecation = new Deprecation(deprecationMessage)
    const errorMessage = `${removalMessage}.\nSee ${deprecation.getDocumentationLink()}`
    recordedErrors.push(errorMessage)
    core.setFailed(errorMessage)
}

export function getDeprecations(): Deprecation[] {
    return recordedDeprecations
}

export function getErrors(): string[] {
    return recordedErrors
}

export function emitDeprecationWarnings(hasJobSummary = true): void {
    if (recordedDeprecations.length > 0) {
        core.warning(
            `This job uses deprecated functionality from the '${getActionId()}' action. Consult the ${hasJobSummary ? 'Job Summary' : 'logs'} for more details.`
        )
        for (const deprecation of recordedDeprecations) {
            core.info(`DEPRECATION: ${deprecation.message}. See ${deprecation.getDocumentationLink()}`)
        }
    }
}

export function saveDeprecationState(): void {
    core.saveState('deprecation-collector_deprecations', JSON.stringify(recordedDeprecations))
    core.saveState('deprecation-collector_errors', JSON.stringify(recordedErrors))
}

export function restoreDeprecationState(): void {
    const savedDeprecations = core.getState('deprecation-collector_deprecations')
    if (savedDeprecations) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        JSON.parse(savedDeprecations).forEach((obj: any) => {
            recordedDeprecations.push(new Deprecation(obj.message))
        })
    }

    const savedErrors = core.getState('deprecation-collector_errors')
    if (savedErrors) {
        recordedErrors.push(...JSON.parse(savedErrors))
    }
}
