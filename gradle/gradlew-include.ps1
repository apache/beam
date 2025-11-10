#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Binary, especially executable binary files, shall not be in the source tree of any Apache project.
# Gradle usually requires a `gradle-wrapper.jar` file to be present in the source tree.
# This script, included from `gradlew` downloads the gradle-wrapper.jar if necessary and verifies its integrity.
# The `gradle-wrapper.jar` and its checksum are downloaded from two well-known locations.

# This fixes an issue that Get-FileHash works fine "out of the box" on older Windows version, but fails on
# newer ones. More info via https://github.com/actions/runner-images/issues/225
Import-Module $PSHOME\Modules\Microsoft.PowerShell.Utility -Function Get-FileHash

# Extract the Gradle version from gradle-wrapper.properties using a regular expression.
$GradlePropertiesPath = Join-Path -Path $env:APP_HOME -ChildPath "gradle\wrapper\gradle-wrapper.properties"
if (-not (Test-Path $GradlePropertiesPath)) {
    Write-Error "Gradle properties file not found: $GradlePropertiesPath"
    exit 1
}

# Read the content and use a regex match to capture the version number.
# Bash regex: 's/^.*gradle-\([0-9.]*\)-[a-z]*.zip$/\1/'
# PowerShell equivalent: Match the distributionUrl line, then capture the version group.
$GRADLE_DIST_VERSION = (
Select-String -Path $GradlePropertiesPath -CaseSensitive `
                  -Pattern 'distributionUrl=' |
        ForEach-Object {
            if ($_ -match 'gradle-([\d.]+)-[a-z]+\.zip') {
                $Matches[1]
            }
        }
)

# Define file paths
$GRADLE_WRAPPER_SHA256 = Join-Path -Path $env:APP_HOME -ChildPath "gradle\wrapper\gradle-wrapper-$GRADLE_DIST_VERSION.jar.sha256"
$GRADLE_WRAPPER_JAR = Join-Path -Path $env:APP_HOME -ChildPath "gradle\wrapper\gradle-wrapper.jar"

# Checksum Verification and Cleanup
# If the checksum file does not exist, delete the wrapper jar.
if (-not (Test-Path $GRADLE_WRAPPER_SHA256)) {
    Remove-Item -Path $GRADLE_WRAPPER_JAR -Force -ErrorAction SilentlyContinue
}

# If the wrapper jar exists, verify its checksum.
if (Test-Path $GRADLE_WRAPPER_JAR) {
    try {
        # Calculate the SHA256 hash of the existing wrapper JAR.
        # Get-FileHash is the native PowerShell equivalent for sha256sum.
        $JarHashObject = Get-FileHash -Path $GRADLE_WRAPPER_JAR -Algorithm SHA256
        $JAR_CHECKSUM = $JarHashObject.Hash.ToLower() # Hash is uppercase by default, convert to lowercase

        # Read the expected checksum from the file.
        # Note: 'cat' is an alias for Get-Content in PowerShell.
        $EXPECTED = (Get-Content -Path $GRADLE_WRAPPER_SHA256 -Raw).Trim().ToLower()

        # Compare checksums and delete files if they do not match.
        if ($JAR_CHECKSUM -ne $EXPECTED) {
            Write-Warning "Checksum mismatch. Deleting $GRADLE_WRAPPER_JAR and $GRADLE_WRAPPER_SHA256."
            Remove-Item -Path $GRADLE_WRAPPER_JAR -Force -ErrorAction SilentlyContinue
            Remove-Item -Path $GRADLE_WRAPPER_SHA256 -Force -ErrorAction SilentlyContinue
        }
    } catch {
        # Handle cases where Get-Content or Get-FileHash might fail (e.g., file deleted during operation).
        Write-Warning "Error during checksum verification: $($_.Exception.Message)"
        Remove-Item -Path $GRADLE_WRAPPER_JAR -Force -ErrorAction SilentlyContinue
        Remove-Item -Path $GRADLE_WRAPPER_SHA256 -Force -ErrorAction SilentlyContinue
    }
}

# Download Checksum File
# If the checksum file is missing, download it.
if (-not (Test-Path $GRADLE_WRAPPER_SHA256)) {
    $Sha256DownloadUrl = "https://services.gradle.org/distributions/gradle-$GRADLE_DIST_VERSION-wrapper.jar.sha256"
    Write-Host "Downloading SHA256 checksum from $Sha256DownloadUrl"
    # Invoke-WebRequest is the native PowerShell equivalent for curl --location --output.
    try {
        Invoke-WebRequest -Uri $Sha256DownloadUrl -OutFile $GRADLE_WRAPPER_SHA256 -UseBasicParsing -ErrorAction Stop
    } catch {
        Write-Error "Failed to download SHA256 checksum: $($_.Exception.Message)"
        exit 1
    }
}

# Download Wrapper JAR and Final Verification
# If the wrapper jar is missing, download it.
if (-not (Test-Path $GRADLE_WRAPPER_JAR)) {
    # The original script handles a case where the version might be like 'x.y' and needs 'x.y.0'.
    # Bash sed: 's/^\([0-9]*[.][0-9]*\)$/\1.0/'
    # PowerShell equivalent using regex replacement.
    $GRADLE_VERSION = $GRADLE_DIST_VERSION
    if ($GRADLE_DIST_VERSION -match '^\d+\.\d+$') {
        $GRADLE_VERSION = "$GRADLE_DIST_VERSION.0"
    }

    $JarDownloadUrl = "https://raw.githubusercontent.com/gradle/gradle/v$GRADLE_VERSION/gradle/wrapper/gradle-wrapper.jar"
    Write-Host "Downloading wrapper JAR from $JarDownloadUrl"

    try {
        Invoke-WebRequest -Uri $JarDownloadUrl -OutFile $GRADLE_WRAPPER_JAR -UseBasicParsing -ErrorAction Stop
    } catch {
        Write-Error "Failed to download wrapper JAR: $($_.Exception.Message)"
        exit 1
    }

    # Verify the checksum of the newly downloaded JAR.
    try {
        $JarHashObject = Get-FileHash -Path $GRADLE_WRAPPER_JAR -Algorithm SHA256
        $JAR_CHECKSUM = $JarHashObject.Hash.ToLower()
        $EXPECTED = (Get-Content -Path $GRADLE_WRAPPER_SHA256 -Raw).Trim().ToLower()

        if ($JAR_CHECKSUM -ne $EXPECTED) {
            # Critical failure: downloaded file does not match its expected checksum.
            Write-Error "Expected sha256 of the downloaded $GRADLE_WRAPPER_JAR does not match the downloaded sha256!"
            exit 1
        }
    } catch {
        Write-Error "Error during final checksum verification: $($_.Exception.Message)"
        exit 1
    }
}