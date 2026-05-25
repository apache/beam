import path from 'path'
import fs from 'fs'

export function readResourceFileAsString(...paths: string[]): string {
    // Resolving relative to __dirname will allow node to find the resource at runtime
    const absolutePath = path.resolve(__dirname, '..', '..', '..', 'sources', 'src', 'resources', ...paths)
    return fs.readFileSync(absolutePath, 'utf8')
}

/**
 * Iterate over all `JAVA_HOME_{version}_{arch}` envs and construct the toolchain.xml.
 *
 * @VisibleForTesting
 */
export function getPredefinedToolchains(): string | null {
    // Get the version and path for each JAVA_HOME env var
    const javaHomeEnvs = Object.entries(process.env)
        .filter(([key]) => key.startsWith('JAVA_HOME_') && process.env[key])
        .map(([key, value]) => ({
            jdkVersion: key.match(/JAVA_HOME_(\d+)_/)?.[1] ?? null,
            jdkPath: value as string
        }))
        .filter(env => env.jdkVersion !== null)

    if (javaHomeEnvs.length === 0) {
        return null
    }

    // language=XML
    return `<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
<!-- JDK Toolchains installed by default on GitHub-hosted runners -->
${javaHomeEnvs
    .map(
        ({jdkVersion, jdkPath}) => `  <toolchain>
    <type>jdk</type>
    <provides>
      <version>${jdkVersion}</version>
    </provides>
    <configuration>
      <jdkHome>${jdkPath}</jdkHome>
    </configuration>
  </toolchain>`
    )
    .join('\n')}
</toolchains>\n`
}

export function mergeToolchainContent(existingToolchainContent: string, preInstalledToolchains: string): string {
    const appendedContent = preInstalledToolchains.split('<toolchains>').pop()!
    return existingToolchainContent.replace('</toolchains>', appendedContent)
}
