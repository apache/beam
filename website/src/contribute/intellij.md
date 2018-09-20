---
layout: section
title: "Beam IntelliJ Tips"
permalink: /contribute/intellij/
section_menu: section-menu/contribute.html
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

# IntelliJ Tips

> These are best-effort community-contributed tips, and are not guaranteed to work with any particular IntelliJ setup. 

## Create a working Gradle module

(as of Intellij 2018.1.6)

1. Create an empty IntelliJ project outside of the Beam source tree.
2. Under Project Structure > Project, select a Project SDK.
3. Under Project Structure > Modules, click the + sign to add a module and
   select "Import Module".
    1. Select the directory containing the Beam source tree.
    2. Tick the "Import module from external model" button and select Gradle
       from the list.
    3. Tick the following boxes.
       * Create separate module per source set
       * Use default gradle wrapper
4. Delegate build actions to Gradle by going to Preferences/Settings > Build, Execution,
   Deployment > Build Tools > Gradle > Runner and checking "Delegate IDE build/run
   actions to gradle".

This should result in a working Gradle project. Build the project by executing
the "build" task in the root Gradle module.

## Checkstyle

IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

Note: Older versions of IntelliJ may not support the Checkstyle file used by Beam.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository
2. Configure the plugin by going to Settings -> Other Settings -> Checkstyle
3. Set Checkstyle version to the same as in `/build_rules.gradle` (e.g. 8.7)
4. Set the "Scan Scope" to "Only Java sources (including tests)"
5. In the "Configuration File" pane, add a new configuration using the plus icon:
    1. Set the "Description" to "Beam"
    2. Select "Use a local Checkstyle file", and point it to
      `sdks/java/build-tools/src/main/resources/beam/checkstyle.xml` within
      your repository
    3. Check the box for "Store relative to project location", and click
      "Next"
    4. Configure the `checkstyle.suppressions.file` property value to
      `suppressions.xml`, and click "Next", then "Finish"
6. Select "Beam" as the only active configuration file, and click "Apply" and
   "OK"
7. Checkstyle will now give warnings in the editor for any Checkstyle
   violations

You can also scan an entire module by opening the Checkstyle tools window and
clicking the "Check Module" button. The scan should report no errors.

Note: Selecting "Check Project" may report some errors from the archetype
modules as they are not configured for Checkstyle validation.

## Code Style

Note: As of release 2.6.0 uniform formatting for Java and Groovy code is automated by the build
through the [Spotless Gradle plugin](https://github.com/diffplug/spotless/tree/master/plugin-gradle).
Instead of relying on the IDE, now you can run `./gradlew spotlessApply`
to reformat changes prior to commit.

IntelliJ supports code styles within the IDE. Use one or both of the following
to ensure your code style matches the project's checkstyle enforcements.

1. The simplest way to have uniform code style is to use the
   [Google Java Format
   plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format)
2. You can also configure IntelliJ to use `beam-codestyle.xml`
    1. Go to Settings -> Code Style -> Java
    2. Click the cogwheel icon next to 'Scheme' and select Import Scheme -> Eclipse XML Profile
    3. Select `sdks/java/build-tools/src/main/resources/beam/beam-codestyle.xml`
    4. Click "OK"
    5. Click "Apply" and "OK"
