import * as core from '@actions/core'
import * as httpm from '@actions/http-client'
import {BuildScanConfig} from '../configuration'
import {recordDeprecation} from '../deprecation-collector'

export async function setupToken(
    develocityAccessKey: string,
    develocityAllowUntrustedServer: boolean | undefined,
    develocityTokenExpiry: string
): Promise<void> {
    if (develocityAccessKey) {
        try {
            core.debug('Fetching short-lived token...')
            const tokens = await getToken(develocityAccessKey, develocityAllowUntrustedServer, develocityTokenExpiry)
            if (tokens != null && !tokens.isEmpty()) {
                core.debug(`Got token(s), setting the access key env vars`)
                const token = tokens.raw()
                core.setSecret(token)
                exportAccessKeyEnvVars(token)
            } else {
                handleMissingAccessToken()
            }
        } catch (e) {
            handleMissingAccessToken()
            core.warning(`Failed to fetch short-lived token, reason: ${e}`)
        }
    }
}

function exportAccessKeyEnvVars(value: string): void {
    ;[BuildScanConfig.DevelocityAccessKeyEnvVar, BuildScanConfig.GradleEnterpriseAccessKeyEnvVar].forEach(key =>
        core.exportVariable(key, value)
    )
}

function handleMissingAccessToken(): void {
    core.warning(`Failed to fetch short-lived token for Develocity`)

    if (process.env[BuildScanConfig.GradleEnterpriseAccessKeyEnvVar]) {
        // We do not clear the GRADLE_ENTERPRISE_ACCESS_KEY env var in v3, to let the users upgrade to DV 2024.1
        recordDeprecation(`The ${BuildScanConfig.GradleEnterpriseAccessKeyEnvVar} env var is deprecated`)
    }
    if (process.env[BuildScanConfig.DevelocityAccessKeyEnvVar]) {
        core.warning(`The ${BuildScanConfig.DevelocityAccessKeyEnvVar} env var should be mapped to a short-lived token`)
    }
}

export async function getToken(
    accessKey: string,
    allowUntrustedServer: undefined | boolean,
    expiry: string
): Promise<DevelocityAccessCredentials | null> {
    const empty: Promise<DevelocityAccessCredentials | null> = new Promise(r => r(null))
    const develocityAccessKey = DevelocityAccessCredentials.parse(accessKey)
    const shortLivedTokenClient = new ShortLivedTokenClient(allowUntrustedServer)

    if (develocityAccessKey == null) {
        return empty
    }
    const tokens = new Array<HostnameAccessKey>()
    for (const k of develocityAccessKey.keys) {
        try {
            core.info(`Requesting short-lived Develocity access token for ${k.hostname}`)
            const token = await shortLivedTokenClient.fetchToken(`https://${k.hostname}`, k, expiry)
            tokens.push(token)
        } catch (e) {
            // Ignore failure to obtain token
            core.info(`Failed to obtain short-lived Develocity access token for ${k.hostname}: ${e}`)
        }
    }
    if (tokens.length > 0) {
        return DevelocityAccessCredentials.of(tokens)
    }
    return empty
}

class ShortLivedTokenClient {
    httpc: httpm.HttpClient
    maxRetries = 3
    retryInterval = 1000

    constructor(develocityAllowUntrustedServer: boolean | undefined) {
        this.httpc = new httpm.HttpClient('gradle/actions/setup-gradle', undefined, {
            ignoreSslError: develocityAllowUntrustedServer
        })
    }

    async fetchToken(serverUrl: string, accessKey: HostnameAccessKey, expiry: string): Promise<HostnameAccessKey> {
        const queryParams = expiry ? `?expiresInHours=${expiry}` : ''
        const sanitizedServerUrl = !serverUrl.endsWith('/') ? `${serverUrl}/` : serverUrl
        const headers = {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${accessKey.key}`
        }

        let attempts = 0
        while (attempts < this.maxRetries) {
            try {
                const requestUrl = `${sanitizedServerUrl}api/auth/token${queryParams}`
                core.debug(`Attempt ${attempts} to fetch short lived token at ${requestUrl}`)
                const response = await this.httpc.post(requestUrl, '', headers)
                if (response.message.statusCode === 200) {
                    const text = await response.readBody()
                    return new Promise<HostnameAccessKey>(resolve => resolve({hostname: accessKey.hostname, key: text}))
                }
                // This should be only 404
                attempts++
                if (attempts === this.maxRetries) {
                    return new Promise((_resolve, reject) =>
                        reject(
                            new Error(
                                `Develocity short lived token request failed ${serverUrl} with status code ${response.message.statusCode}`
                            )
                        )
                    )
                }
            } catch (error) {
                attempts++
                if (attempts === this.maxRetries) {
                    return new Promise((_resolve, reject) => reject(error))
                }
            }
            await new Promise(resolve => setTimeout(resolve, this.retryInterval))
        }
        return new Promise((_resolve, reject) => reject(new Error('Illegal state')))
    }
}

type HostnameAccessKey = {
    hostname: string
    key: string
}

export class DevelocityAccessCredentials {
    static readonly accessKeyRegexp = /^([^;=\s]+=\w+)(;[^;=\s]+=\w+)*$/
    readonly keys: HostnameAccessKey[]

    private constructor(allKeys: HostnameAccessKey[]) {
        this.keys = allKeys
    }

    static of(allKeys: HostnameAccessKey[]): DevelocityAccessCredentials {
        return new DevelocityAccessCredentials(allKeys)
    }

    private static readonly keyDelimiter = ';'
    private static readonly hostDelimiter = '='

    static parse(rawKey: string): DevelocityAccessCredentials | null {
        if (!this.isValid(rawKey)) {
            return null
        }
        return new DevelocityAccessCredentials(
            rawKey.split(this.keyDelimiter).map(hostKey => {
                const pair = hostKey.split(this.hostDelimiter)
                return {hostname: pair[0], key: pair[1]}
            })
        )
    }

    isEmpty(): boolean {
        return this.keys.length === 0
    }

    raw(): string {
        return this.keys
            .map(k => `${k.hostname}${DevelocityAccessCredentials.hostDelimiter}${k.key}`)
            .join(DevelocityAccessCredentials.keyDelimiter)
    }

    private static isValid(allKeys: string): boolean {
        return this.accessKeyRegexp.test(allKeys)
    }
}
