import {afterAll, describe, expect, it, jest} from '@jest/globals'

import {getPredefinedToolchains, mergeToolchainContent} from "../../src/caching/gradle-user-home-utils";

describe('predefined-toolchains', () => {
    const OLD_ENV = process.env
    afterAll(() => {
        process.env = OLD_ENV
    });

    describe('returns', () => {
        it('null if no JAVA_HOME_ envs are set', async () => {
            jest.resetModules()
            process.env = {
                "JAVA_HOME": "/jdks/foo_8"
            }

            const predefinedToolchains = getPredefinedToolchains()
            expect(predefinedToolchains).toBe(null)
        })
        it('valid toolchains.xml if JAVA_HOME_ envs are set', async () => {
            jest.resetModules()
            process.env = {
                "JAVA_HOME": "/jdks/foo_8",
                "JAVA_HOME_8_X64": "/jdks/foo_8",
                "JAVA_HOME_11_X64": "/jdks/foo_11",
                "JAVA_HOME_21_ARM64": "/jdks/foo_21",
            }

            const predefinedToolchains = getPredefinedToolchains()
            expect(predefinedToolchains).toBe(
                // language=XML
`<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
<!-- JDK Toolchains installed by default on GitHub-hosted runners -->
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>8</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_8</jdkHome>
    </configuration>
  </toolchain>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>11</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_11</jdkHome>
    </configuration>
  </toolchain>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>21</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_21</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
`)
        })
    })

    it("merges with existing toolchains", async () => {
        jest.resetModules()
        process.env = {
            "JAVA_HOME_11_X64": "/jdks/foo_11",
        }

        // language=XML
        const existingToolchains =
            `<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>8</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_8</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
`

        const mergedContent = mergeToolchainContent(existingToolchains, getPredefinedToolchains()!)
        expect(mergedContent).toBe(
            // language=XML
            `<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>8</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_8</jdkHome>
    </configuration>
  </toolchain>

<!-- JDK Toolchains installed by default on GitHub-hosted runners -->
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>11</version>
    </provides>
    <configuration>
      <jdkHome>/jdks/foo_11</jdkHome>
    </configuration>
  </toolchain>
</toolchains>

`)
    })
})
