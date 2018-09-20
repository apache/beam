---
layout: section
title: "Beam Eclipse Tips"
permalink: /contribute/eclipse/
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

# Eclipse Tips

> These are best-effort community-contributed tips, and are not guaranteed to work with any particular Eclipse setup.

## Eclipse version

Use a recent Eclipse version that includes
[Buildship](https://projects.eclipse.org/projects/tools.buildship) for Gradle
integration. Currently we recommend Eclipse Oxygen. Start Eclipse with a fresh
workspace in a separate directory from your checkout.

## Initial setup

Before setting up Eclipse integration, verify that you can successfully build
from the commandline by building a sample package:

```
./gradlew :beam-examples-java:build
```

If you receive any errors, first verify your environment setup:

1. If running on Mac or Linux, launch Eclipse from a terminal. This is necessary
   to ensure that Eclipse has proper environment setup from user profile
   scripts, i.e. .bashrc.
2. Install [Buildship Gradle
   Integration](https://marketplace.eclipse.org/content/buildship-gradle-integration).
   This will allow importing and interacting with the Gradle build.
3. Open the project import wizard in Eclipse via "File" > "Import".
4. From the "Import" screen, select "Gradle" > "Existing Gradle Project", and click
   Next.
5. From the "Import Gradle Project" screen, fill in the Project root directory
   with your local git path, and click Finish.

Eclipse will scan the project tree and import each as a separate Package.

Verify that your workspace is correctly configured by invoking
'beam-runners-direct-java:build' from the "Gradle Tasks" pane. The build should
succeed with no errors.

## Building

After your Eclipse workspace is properly setup, you will have a "Gradle
Tasks" window with a set of operations. If you don't see the pane, open it
from "Window" > "Show View" > Other.. > "Gradle" > "Gradle Tasks".

From the "Gradle Tasks" window, you can build any task registered with Gradle.
For example, if you are working on Kinesis IO, select 'beam-sdks-java-io-kinesis:build'.

## Checkstyle

Eclipse supports checkstyle within the IDE using the Checkstyle plugin.

1. Install the [Checkstyle
   plugin](https://marketplace.eclipse.org/content/checkstyle-plug).
2. Configure the Checkstyle plugin by going to Preferences -> Checkstyle.
    1. Click "New..."
    2. Select "External Configuration File" for type
    3. Click "Browse..." and select
       `sdks/java/build-tools/src/main/resources/beam/checkstyle.xml`
    4. Enter "Beam Checks" under "Name:"
    5. Click "OK", then "OK"

## Code Style

Eclipse supports code styles within the IDE. Use one or both of the following
to ensure your code style matches the project's checkstyle enforcement.

1. The simplest way to have uniform code style is to use the [Google
   Java Format plugin](https://github.com/google/google-java-format#eclipse)
2. You can also configure Eclipse to use `beam-codestyle.xml`
    1. Go to Preferences -> Java -> Code Style -> Formatter
    2. Click "Import..." and select
       `sdks/java/build-tools/src/main/resources/beam/beam-codestyle.xml`
    3. Click "Apply" and "OK"

