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

The Beam SDKs depend on common third-party components which then
import additional dependencies. Version collisions can result in unexpected
behavior in the service. If you are using any of these packages in your code, be
aware that some libraries are not forward-compatible and you may need to pin to
the listed versions that will be in scope during execution.

Compile and runtime dependencies for your Beam SDK version are listed in `BeamModulePlugin.groovy` in the Beam repository. To view them, perform the following steps:

1. Open `BeamModulePlugin.groovy`.

    ```
    https://raw.githubusercontent.com/apache/beam/v<VERSION_NUMBER>/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
    ```

    Replace `<VERSION_NUMBER>` with the major.minor.patch version of the SDK. For example, <https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy> will provide the dependencies for the {{< param release_latest >}} release.
    
2. Review the list under `project.ext.library`. 

    **Note:** Some dependencies in the list use version variables, such as `google_auth_version`. These variables are defined prior to the `project.ext.library` map definition.

You can also retrieve this list by creating a new project through Maven and resolving the dependencies.

1. Define the Beam SDK and Java versions for the new project. 

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

