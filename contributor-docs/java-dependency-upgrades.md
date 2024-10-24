<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Upgrading a Java Dependency

To perform a dependency upgrade we want to ensure that the PR is not
introducing any new [linkage errors](https://jlbp.dev/glossary). We do this by
combining successful Jenkins test runs with analysis performed using a linkage
checker. This allows us to gain confidence that we are minimizing the number of
linkage issues that will arise for users. To perform a dependency upgrade:

 - Find all Gradle subprojects that are impacted by the dependency change.
 - For each Gradle subproject impacted by a dependency change:
     - Perform the before and after linkage checker analysis.
     - Provide the results as part of your PR.
 - For each Gradle subproject:
     - Find and run relevant Jenkins test suites.

See the following sections for how step-by-step instructions.

## How to find all Gradle subprojects that are impacted by the dependency change

Execute the command below will print out a dependency report in a text file for
each project:

    ./gradlew dependencyReport

Grep for a specific maven artifact identifier such as guava in all the
dependency reports with:

    grep -l "guava" `find ./ -name dependencies.txt`

## Linkage checker analysis

> :warning: This step relies on modifying your local maven repository,
> typically found in ~/.m2/.

Use the shell script to do this on your behalf (note that it will run the
manual command below on your current workspace and also on HEAD):

    /bin/bash sdks/java/build-tools/beam-linkage-check.sh origin/master <your branch name> "artifactId1,artifactId2,..."

> :warning: If you omit the artifactIds, it uses beam-sdks-java-core
> beam-sdks-java-io-google-cloud-platform
> beam-runners-google-cloud-dataflow-java beam-sdks-java-io-hadoop-format;
> these artifacts often suffer dependency conflicts.

Copy and paste the output to the PR. If it is large, you may want to use a GitHub gist. For example PRs (1, 2, 3, 4, and 5).

Note that you can manually run the linkage checker on your current workspace by invoking:

    ./gradlew -Ppublishing -PjavaLinkageArtifactIds=artifactId1,artifactId2,... :checkJavaLinkage

Check the example output is:

```
Class org.brotli.dec.BrotliInputStream is not found;
  referenced by 1 class file
    org.apache.beam.repackaged.core.org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream (beam-sdks-java-core-2.20.0-SNAPSHOT.jar)
Class com.github.luben.zstd.ZstdInputStream is not found;
  referenced by 1 class file
    org.apache.beam.repackaged.core.org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream (beam-sdks-java-core-2.20.0-SNAPSHOT.jar)
Class com.github.luben.zstd.ZstdOutputStream is not found;
  referenced by 1 class file
    org.apache.beam.repackaged.core.org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream (beam-sdks-java-core-2.20.0-SNAPSHOT.jar)
Class org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.jar.asm.commons.ModuleHashesAttribute is not found;
  referenced by 1 class file
    org.apache.beam.vendor.bytebuddy.v1_9_3.net.bytebuddy.jar.asm.commons.ClassRemapper (beam-vendor-bytebuddy-1_9_3-0.1.jar)
```

Delete any installed Apache Beam SNAPSHOT artifacts:

    rm -rf ~/.m2/repository/org/apache/beam

## Run relevant Jenkins test suites and GitHub Actions

You can find all Jenkins job configurations within
https://github.com/apache/beam/tree/master/.test-infra/jenkins and request that
the reviewer run the relevant test suites by providing them with a list of all
the relevant trigger phrases. You can perform this request directly on your PR
or on the dev mailing list, for [example](https://lists.apache.org/thread/jgjdt52jm6rk0ndrjjnvk1nn65dl9358).

# Google Cloud-related dependency upgrades

Beam uses [GCP-BOM](https://cloud.google.com/java/docs/bom) to manage Google Cloud-related dependencies. A [script](../scripts/tools/bomupgrader.py) is used to upgrade GCP-BOM dependencies. To upgrade, find the latest BOM version in https://mvnrepository.com/artifact/com.google.cloud/libraries-bom and run

```
python scripts/tools/bomupgrader.py <latest_bom_version>
```

then the changes will made in place.

This script does the following steps:

1. preprocessing BeamModulePlugin.groovy to decide the dependencies need to sync
2. generate an empty Maven project to fetch the exact target versions to change
3. Write back to BeamModulePlugin.groovy
4. Update libraries-bom version on sdks/java/container/license_scripts/dep_urls_java.yaml

In the case of modifying the script or diagnose, the following commands may be useful to identify matching/consistent dependency overrides.

    export BOM_VERSION=26.22.0 ; \
    cd /tmp; \
    wget https://repo1.maven.org/maven2/com/google/cloud/libraries-bom/$BOM_VERSION/libraries-bom-$BOM_VERSION.pom -O base.pom && \
    mvn help:effective-pom -f base.pom -Doutput=effective.pom && cat effective.pom | \
    grep -v 'dependencyManagement' > cleanup.pom && \
    mvn dependency:tree -f cleanup.pom
