---
type: languages
title: "Java SDK dependencies"
---
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

# Beam SDK for Java dependencies

The Apache Beam SDKs depend on common third-party components. These components
import additional dependencies. You need to manage your dependencies for the following reasons:

- Dependencies might have version collisions or incompatible classes and libraries.
- Some libraries are not forward compatible. When you use these packages in your code,
  you might need to pin to the appropriate versions so that those versions are used
  when you run your pipeline.

When problems occur with dependencies, you might see unexpected behavior in the service,
including errors such as `NoClassDefFoundError`, `NoSuchMethodError`, `NoSuchFieldError`,
or `FATAL ERROR in native method`.

This page explains how to view the dependencies that your SDK is using and how to manage your
dependencies to avoid issues.

## View dependencies

To view your dependencies, either use the
`BeamModulePlugin.groovy` file or retrieve the list by creating a new project
through Maven and resolving the dependencies.

### Use BeamModulePlugin.groovy to retrieve dependencies

The `BeamModulePlugin.groovy` file in the Beam repository lists compile and runtime
dependencies for your Beam SDK version.

1. Use the following link to open the `BeamModulePlugin.groovy` file.

    ```
    https://raw.githubusercontent.com/apache/beam/vBEAM_VERSION/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
    ```

    <p class="paragraph-wrap">Replace <em>BEAM_VERSION</em> with the SDK version
        that you're using. The following example provides the dependencies for the
        {{< param release_latest >}} release: <a href="https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy" target="_blank" rel="noopener noreferrer">https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy</a>.</p>

2. Under `project.ext.library`, review the list of dependencies. Some dependencies in the
list use version variables, such as `google_cloud_bigdataoss_version`. These variables are
defined before the `project.ext.library` map definition.

### Use a Maven project to resolve dependencies

You can retrieve the list of dependencies by creating a new project through Maven and
then resolving the dependencies.

1. In your terminal or command line, use the following command to define the Beam SDK and Java versions for the new project.

    ```
    export BEAM_VERSION={{< param release_latest >}}
    export JAVA_VERSION=11
    ```

2. Create the project.

    ```
    mvn archetype:generate \
        -DinteractiveMode=false \
        -DarchetypeGroupId=org.apache.beam \
        -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
        -DarchetypeVersion=$BEAM_VERSION \
        -DtargetPlatform=$JAVA_VERSION \
        -DartifactId=check-pipeline-dependencies \
        -DgroupId=org.apache.beam.samples
    ```

3. Change to the new project directory.

    ```
    cd check-pipeline-dependencies
    ```

4. Resolve and list the dependencies.

    ```
    mvn dependency:resolve && mvn -o dependency:list
    ```

## Manage dependencies

To simplify dependency management, Beam provides
[Bill of Materials (BOM)](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#bill-of-materials-bom-poms)
artifacts that help dependency management tools select compatible combinations.

When you import Apache Beam, using a Bill of Material artifact is recommended.
When a project import contains unspecified or ambiguous dependencies,
the BOM provides the information that the SDK needs to use the correct
dependency version.

Apache Beam provides two BOMs:

- `beam-sdks-java-bom`: manages Apache Beam dependencies, which allows
  you to specify the version only one time
- `beam-sdks-java-io-google-cloud-platform-bom`: manages Apache Beam, Google Cloud,
  and third-party dependencies

Because errors are more likely to occur when you use third-party dependencies,
the `beam-sdks-java-io-google-cloud-platform-bom` BOM is recommended.

### Import the BOM

To use a BOM, import the BOM into your Maven or Gradle
dependency configuration. For example, to
use `beam-sdks-java-io-google-cloud-platform-bom`,
make the following changes in the `pom.xml` file of your SDK artifact.
In the following examples, replace _BEAM_VERSION_ with the appropriate
Apache Beam SDK version.

**Maven**

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-sdks-java-google-cloud-platform-bom</artifactId>
      <version>BEAM_VERSION</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

**Gradle**

```
dependencies {
    implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:BEAM_VERSION"))
}
```

### Remove version pinning

After you import the BOM, you can remove specific version pinning from your dependencies. For example,
you can remove the versions associated with `org.apache.beam`, `io.grpc`, and `com.google.cloud`,
including `libraries-bom`. Because the dependencies aren't automatically imported by the BOM,
don't remove them entirely. Keep the dependency without specifying a version.

The following example shows dependencies without versions in Maven:

```xml
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-core</artifactId>
</dependency>
```

The following example shows dependencies without versions in Gradle:

```
implementation("org.apache.beam:beam-sdks-java-core")
```

### View the depenencies managed by the BOM

To see a full list of dependency versions that are managed by a specific BOM, use the
Maven tool `help:effective-pom` by running the following command.
Replace _BEAM_VERSION_ with the appropriate Apache Beam SDK version.

```shell
mvn help:effective-pom -f ~/.m2/repository/org/apache/beam/beam-sdks-java-google-cloud-platform-bom/BEAM_VERSION/beam-sdks-java-google-cloud-platform-bom-BEAM_VERSION.pom
```

## Resources

- [Beam SDKs Java Google Cloud Platform BOM](https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-google-cloud-platform-bom/) in the Maven repository.