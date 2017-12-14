---
layout: section
title: "Beam IntelliJ Tips"
permalink: /contribute/intellij/
section_menu: section-menu/contribute.html
---

# IntelliJ Tips

> These are best-effort community-contributed tips, and are not...
>
> - ... guaranteed to work with any particular IntelliJ setup.
> - ... the only or best way to work.
> - ... an endorsement of IntelliJ over alternatives.
>
> Mastering IntelliJ is, ultimately, your responsibility.

## Enable Annotation Processing

To configure annotation processing in IntelliJ:

1. Open Annotation Processors Settings dialog box by going to Settings ->
   Build, Execution, Deployment -> Compiler -> Annotation Processors
2. Select the following buttons:
   * "Enable annotation processing"
   * "Obtain processors from project classpath"
   * "Store generated sources relative to: _Module content root_"
3. Set the generated source directories to be equal to the Maven directories:
   * Set "Production sources directory:" to `target/generated-sources/annotations`
   * Set "Test sources directory:" to `target/generated-test-sources/test-annotations`
4. Click "OK"

## Checkstyle

IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository
2. Configure the plugin by going to Settings -> Other Settings -> Checkstyle
3. Set Checkstyle version to the same as in `/pom.xml` (e.g. 6.19)
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

