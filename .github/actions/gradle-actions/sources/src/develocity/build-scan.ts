import * as core from '@actions/core'
import {BuildScanConfig} from '../configuration'
import {setupToken} from './short-lived-token'

export async function setup(config: BuildScanConfig): Promise<void> {
    maybeExportVariable('DEVELOCITY_INJECTION_INIT_SCRIPT_NAME', 'gradle-actions.inject-develocity.init.gradle')
    maybeExportVariable('DEVELOCITY_INJECTION_CUSTOM_VALUE', 'gradle-actions')

    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_ENABLED', config.getDevelocityInjectionEnabled())
    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_URL', config.getDevelocityUrl())
    maybeExportVariableNotEmpty(
        'DEVELOCITY_INJECTION_ALLOW_UNTRUSTED_SERVER',
        config.getDevelocityAllowUntrustedServer()
    )
    maybeExportVariableNotEmpty(
        'DEVELOCITY_INJECTION_CAPTURE_FILE_FINGERPRINTS',
        config.getDevelocityCaptureFileFingerprints()
    )
    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_ENFORCE_URL', config.getDevelocityEnforceUrl())
    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_DEVELOCITY_PLUGIN_VERSION', config.getDevelocityPluginVersion())
    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_CCUD_PLUGIN_VERSION', config.getDevelocityCcudPluginVersion())
    maybeExportVariableNotEmpty('DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_URL', config.getPluginRepository().getUrl())
    maybeExportVariableNotEmpty(
        'DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_USERNAME',
        config.getPluginRepository().getUsername()
    )
    maybeExportVariableNotEmpty(
        'DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_PASSWORD',
        config.getPluginRepository().getPassword()
    )

    // If build-scan-publish is enabled, ensure the environment variables are set
    // The DEVELOCITY_PLUGIN_VERSION and DEVELOCITY_CCUD_PLUGIN_VERSION are set to the latest versions
    // except if they are defined in the configuration
    if (config.getBuildScanPublishEnabled()) {
        maybeExportVariable('DEVELOCITY_INJECTION_ENABLED', 'true')
        maybeExportVariable('DEVELOCITY_INJECTION_DEVELOCITY_PLUGIN_VERSION', '4.2')
        maybeExportVariable('DEVELOCITY_INJECTION_CCUD_PLUGIN_VERSION', '2.1')
        maybeExportVariable('DEVELOCITY_INJECTION_TERMS_OF_USE_URL', config.getBuildScanTermsOfUseUrl())
        maybeExportVariable('DEVELOCITY_INJECTION_TERMS_OF_USE_AGREE', config.getBuildScanTermsOfUseAgree())
    }

    return setupToken(
        config.getDevelocityAccessKey(),
        config.getDevelocityAllowUntrustedServer(),
        config.getDevelocityTokenExpiry()
    )
}

function maybeExportVariable(variableName: string, value: unknown): void {
    if (!process.env[variableName]) {
        core.exportVariable(variableName, value)
    }
}

function maybeExportVariableNotEmpty(variableName: string, value: unknown): void {
    if (value !== null && value !== undefined && value !== '') {
        maybeExportVariable(variableName, value)
    }
}
