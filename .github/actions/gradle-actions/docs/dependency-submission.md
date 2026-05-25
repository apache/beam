# The `dependency-submission` action

The `gradle/actions/dependency-submission` action provides the simplest (and recommended) way to generate a 
dependency graph for your project. This action will attempt to detect all dependencies used by your build
without building and testing the project itself.

The dependency graph snapshot is generated via integration with the [GitHub Dependency Graph Gradle Plugin](https://plugins.gradle.org/plugin/org.gradle.github-dependency-graph-gradle-plugin), and submitted to your repository via the 
[GitHub Dependency Submission API](https://docs.github.com/en/rest/dependency-graph/dependency-submission).
The generated snapshot files can be submitted in the same job, or saved for submission in a subsequent job.

The generated dependency graph includes all of the dependencies in your build, and is used by GitHub to generate 
[Dependabot Alerts](https://docs.github.com/en/code-security/dependabot/dependabot-alerts/about-dependabot-alerts) 
for vulnerable dependencies, as well as to populate the 
[Dependency Graph insights view](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/exploring-the-dependencies-of-a-repository#viewing-the-dependency-graph).

If you're confused by the behaviour you're seeing or have specific questions, please check out [the FAQ](dependency-submission-faq.md) before raising an issue.

## General usage

The following workflow will generate a dependency graph for a Gradle project and submit it immediately to the repository via the
Dependency Submission API. For most projects, this default configuration should be all that you need.

Simply add this as a new workflow file to your repository (eg `.github/workflows/dependency-submission.yml`).

```yaml
name: Dependency Submission

on:
  push:
    branches: [ 'main' ]

permissions:
  contents: write

jobs:
  dependency-submission:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Generate and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
```

### Gradle execution

To generate a dependency graph, the `dependency-submission` action must perform a Gradle execution that resolves
the dependencies of the project. All dependencies that are resolved in this execution will be included in the 
generated dependency graph.  By default action executes a built-in task that is designed to resolve all build dependencies
(`:ForceDependencyResolutionPlugin_resolveAllDependencies`).

The action looks for a Gradle project in the root of the workspace, and executes this project with
the Gradle wrapper, if configured for the project. If the wrapper is not configured, whatever `gradle` available
on the command-line will be used.

The action provides the ability to override the Gradle version and task to execute, as well as provide 
additional arguments that will be passed to Gradle on the command-line. See [Configuration Parameters](#configuration-parameters) below.

### Publishing a Develocity Build Scan® from your dependency submission workflow

You can automatically publish a free Develocity Build Scan on every run of `gradle/actions/dependency-submission`. 
Three input parameters are required, one to enable publishing and two more to accept the 
[Develocity terms of use](https://gradle.com/help/legal-terms-of-use).

```yaml
    - name: Generate and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
      with:
        build-scan-publish: true
        build-scan-terms-of-use-url: "https://gradle.com/help/legal-terms-of-use"
        build-scan-terms-of-use-agree: "yes"
```

A Build Scan makes it easy to determine the source of any dependency vulnerabilities in your project.

### Configuration parameters

In some cases, the default action configuration will not be sufficient, and additional action parameters will need to be specified.

```yaml
    - name: Generate and save dependency graph
      uses: gradle/actions/dependency-submission@v4
      with:
        # Use a particular Gradle version instead of the configured wrapper.
        gradle-version: '8.6'

        # The gradle project is not in the root of the repository.
        build-root-directory: my-gradle-project

        # Choose a task that will trigger dependency resolution
        dependency-resolution-task: myDependencyResolutionTask

        # Additional arguments that should be passed to execute Gradle
        additional-arguments: --no-configuration-cache

        # Enable configuration-cache reuse for this build.
        cache-encryption-key: ${{ secrets.GRADLE_ENCRYPTION_KEY }}

        # Do not attempt to submit the dependency-graph. Save it as a workflow artifact.
        dependency-graph: generate-and-upload

        # Change the number of days that workflow artifacts are retained. (Default is 30 days).
        artifact-retention-days: 5

        # Specify the location where dependency graph files will be generated.
        dependency-graph-report-dir: custom-report-dir

        # By default, failure to generate a dependency graph will cause the workflow to fail
        dependency-graph-continue-on-failure: true

```

See the [Action Metadata file](../dependency-submission/action.yml) for a more detailed description of each input parameter.

The `GitHub Dependency Graph Gradle Plugin` can be further 
[configured via a number of environment variables](https://github.com/gradle/github-dependency-graph-gradle-plugin?#required-environment-variables). 
These will be automatically set by the `dependency-submission` action, but you may override these values 
by setting them explicitly in your workflow file.

### Reducing storage costs for saved dependency graph artifacts

By default, the dependency graph that is generated is stored as a workflow artifact.
To reduce storage costs for these artifacts, you can:

1. Set the `artifact-retention-days`:

```yaml
    - name: Generate dependency graph but only store workflow artifacts for 1 day
      uses: gradle/actions/dependency-submission@v4
      with:
        artifact-retention-days: 1 # Default is 30 days or as configured for repository
```

2. Disable storing dependency-graph artifacts using `generate-and-submit`

```yaml
    - name: Generate and submit dependency graph but do not store as workflow artifact
      uses: gradle/actions/dependency-submission@v4
      with:
        dependency-graph: 'generate-and-submit' # Default value is 'generate-submit-and-upload'
```

# Resolving a dependency vulnerability

## Finding the source of a dependency vulnerability

Once you have submitted a dependency graph, you may receive Dependabot Alerts warning about vulnerabilities in
dependencies of your project. In the case of transitive dependencies, it may not be obvious how that dependency is
used or what you can do to address the vulnerability alert.

The first step to investigating a Dependabot Alert is to determine the source of the dependency. One of the best ways to 
do so is with a free Develocity Build Scan®, which makes it easy to explore the dependencies resolved in your build.

<img width="1069" alt="image" src="https://github.com/gradle/actions/assets/179734/3a637dfd-396c-4e94-8332-dcc6eb5a35ac">

In this example, we are searching for dependencies matching the name 'com.squareup.okio:okio' in the _Build Dependencies_ of 
the project. You can easily see that this dependency originates from 'com.github.ben-manes:gradle-versions-plugin'.
Knowing the source of the dependency can help determine how to deal with the Dependabot Alert.

Note that you may need to look at both the _Dependencies_ and the _Build Dependencies_ of your project to find the
offending dependency.

### When you cannot publish a Build Scan®

If publishing a free Build Scan to https://scans.gradle.com isn't an option, and you don't have access to a private [Develocity
server](https://gradle.com/) for your project, you can obtain information about the each resolved dependency by running the `dependency-submission` workflow with debug logging enabled.

The simplest way to do so is to re-run the dependency-submission job with debug logging enabled:

<img width="665" alt="image" src="https://github.com/gradle/actions/assets/179734/d95b889a-09fb-4731-91f2-baebbf647e31">

When you do so, the Gradle build that generates the dependency-graph will include a log message for each dependency version included in the graph.
Given the details in one log message, you can run (locally) the built-in [dependencyInsight](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html#dependency_insights) task
to determine exactly how the dependency was resolved. 

For example, given the following message in the logs:
```
Detected dependency 'com.google.guava:guava:32.1.3-jre': project = ':my-subproject', configuration = 'compileClasspath'
```

You would run the following command locally:
```
./gradlew :my-subproject:dependencyInsight --configuration compileClasspath --dependency com.google.guava:guava:32.1.3-jre
```

#### Dealing with 'classpath' configuration

If the configuration value in the log message is "classpath" then instead of running `dependency-insight` you'll need to run the Gradle
`buildEnvironment` task.

For example, given the following message in the logs:
```
Detected dependency 'xerces:xercesImpl:2.12.2': project = ':my-subproject', configuration = 'classpath'
```

You would run the following command locally to expose the `xercesImpl` dependency:
```
./gradlew :my-subproject:buildEnvironment | grep -C 5 xercesImpl
```

## Updating the dependency version

Once you've discovered the source of the dependency, the most obvious fix is to update the dependency to a patched version that does not
suffer the vulnerability. For direct dependencies, this is often straightforward.  But for transitive dependencies it can be tricky.

### Dependency source is specified directly in the build

If the dependency is used to compile your code or run your tests, it's normal for the underlying "source" of the dependency to have a
version configured directly in the build. For example, if you have a vulnerable version of `com.squareup.okio:okio` in your `compileClasspath`, then
it's likely you have a dependency like `com.squareup.moshi:moshi` configured as an `api` or `implementation` dependency.

In this case there are 2 possibilities:
1. There is a newer, compatible version of `com.squareup.moshi:moshi` available, and you can just bump the version number.
2. There isn't a newer, compatible version of `com.squareup.moshi:moshi`

In the second case, you can add a Dependency Constraint, to force the use of the newest version of `com.squareup.okio`:

```kotlin
dependencies {
  implementation("com.squareup.moshi:moshi:1.12.0")
  constraints {
    // Force a newer version of okio in transitive resolution
    implementation("com.squareup.okio:okio:3.6.0")
  }
}
```

### Dependency source is a plugin classpath

If the vulnerable dependency is introduced by a Gradle plugin, again the best option is to look for a newer version of the plugin.
But if none is available, you can still use a dependency constraint to force a newer transitive version to be used.

The dependency constraint must be added to the `classpath` configuration of the buildscript that loads the plugin.

```kotlin
buildscript {
  repositories {
    gradlePluginPortal()
  }
  dependencies {
    constraints {
      // Force a newer version of okio in transitive resolution
      classpath("com.squareup.okio:okio:3.6.0")
    }
  }
}
plugins {
  id("com.github.ben-manes.versions") version("0.51.0")
}
```

## Limiting the dependencies that appear in the dependency graph

By default, the `dependency-submission` action attempts to detect all dependencies declared and used by your Gradle build.
At times it may helpful to limit the dependencies reported to GitHub, to avoid security alerts for dependencies that 
don't form a critical part of your product. For example, a vulnerability in the tool you use to generate documentation 
may not be as important as a vulnerability in one of your runtime dependencies.

The `dependency-submission` action provides a convenient mechanism to filter the projects and configurations that
contribute to the dependency graph.

> [!NOTE]
> Ideally, all dependencies involved in building and testing a project will be extracted and reported in a dependency graph. 
> These dependencies would be assigned to different scopes (eg development, runtime, testing) and the GitHub UI would make it easy to opt-in to security alerts for different dependency scopes.
> However, this functionality does not yet exist.

### Selecting Gradle projects that will contribute to the dependency graph

If you do not want the dependency graph to include dependencies from every project in your build, 
you can easily exclude or include certain projects from the dependency extraction process.

To restrict which Gradle subprojects contribute to the report, specify which projects to exclude or include via a regular expression.
You can use the `dependency-graph-exclude-projects` and `dependency-graph-include-projects` input parameters for this purpose.

Note that excluding a project in this way only removes dependencies that are _resolved_ as part of that project, and may
not necessarily remove all dependencies _declared_ in that project. If another project depends on the excluded project
then it may transitively resolve dependencies declared in the excluded project: these dependencies will still be included
in the generated dependency graph.

### Selecting Gradle configurations that will contribute to the dependency graph

Similarly to Gradle projects, it is possible to exclude or include a set of dependency configurations from dependency graph generation,
so that only dependencies resolved by the included configurations are reported.

To restrict which Gradle configurations contribute to the report, specify which configurations to exclude or include via a regular expression.
You can use the `dependency-graph-exclude-configurations` and `dependency-graph-include-configurations` input parameters for this purpose.

Note that configuration exclusion applies to the configuration in which the dependency is _resolved_ which is not necessarily
the configuration where the dependency is _declared_. For example if you decare a dependency as `implementation` in
a Java project, that dependency will be resolved in `compileClasspath`, `runtimeClasspath` and possibly other configurations.

### Example of project and configuration filtering

For example, if you want to exclude dependencies resolved by the `buildSrc` project, and exclude dependencies from the `testCompileClasspath` and `testRuntimeClasspath` configurations, you would use the following configuration:

```yaml
    - name: Generate and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
      with:
        # Exclude all dependencies that originate solely in the 'buildSrc' project
        dependency-graph-exclude-projects: ':buildSrc'
        # Exclude dependencies that are only resolved in test classpaths
        dependency-graph-exclude-configurations: '.*[Tt]est(Compile|Runtime)Classpath'
```

# Advance usage scenarios

## Using a custom plugin repository

By default, the action downloads the `github-dependency-graph-gradle-plugin` from the Gradle Plugin Portal (https://plugins.gradle.org). If your GitHub Actions environment does not have access to this URL, you can specify a custom plugin repository to use with an environment variable.

See [the setup-gradle docs](setup-gradle.md#using-a-custom-plugin-repository) for details.

## Integrating the `dependency-review-action`

The GitHub [dependency-review-action](https://github.com/actions/dependency-review-action) helps you 
understand dependency changes (and the security impact of these changes) for a pull request,
by comparing the dependency graph for the pull-request with that of the HEAD commit.

Integrating the Dependency Review Action requires 2 changes to your workflows:

#### 1. Add a `pull_request` trigger to your existing Dependency Submission workflow.

In order to perform Dependency Review on a pull request, the dependency graph must be submitted for the pull request.
To do this, simply add a `pull_request` trigger to your existing dependency submission workflow.

```yaml
name: Dependency Submission

on:
  push:
    branches: [ 'main' ]
  pull_request:

permissions:
  contents: write

jobs:
  dependency-submission:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Generate and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
```

#### 2. Add a dedicated Dependency Review workflow

The Dependency Review workflow will be triggered directly on `pull_request`, but will wait until the dependency graph results are
submitted before the dependency review can complete. The period to wait is controlled by the `retry-on-snapshot-warnings` input parameters.

Here's an example of a separate "Dependency Review" workflow that will wait up to 10 minutes for dependency submission to complete.

```yaml
name: Dependency Review

on:
  pull_request:

permissions:
  contents: read

jobs:
  dependency-review:
    runs-on: ubuntu-latest
    steps:
    - name: 'Dependency Review'
      uses: actions/dependency-review-action@v4
      with:
        retry-on-snapshot-warnings: true
        retry-on-snapshot-warnings-timeout: 600
```

The `retry-on-snapshot-warnings-timeout` (in seconds) needs to be long enough to allow the modified dependency-submission workflow to complete.

## Usage with pull requests from public forked repositories

This `contents: write` permission is [not available for any workflow that is triggered by a pull request submitted from a public forked repository](https://docs.github.com/en/actions/security-guides/automatic-token-authentication#permissions-for-the-github_token).
This limitation is designed to prevent a malicious pull request from effecting repository changes.

Because of this restriction, we require 2 separate workflows in order to generate and submit a dependency graph:
1. The first workflow runs directly against the pull request sources and will `generate-and-upload` the dependency graph.
2. The second workflow is triggered on `workflow_run` of the first workflow, and will `download-and-submit` the previously saved dependency graph.

***Main workflow file***
```yaml
name: Generate and save dependency graph

on:
  pull_request:

permissions:
  contents: read # 'write' permission is not available

jobs:
  dependency-submission:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Generate and save dependency graph
      uses: gradle/actions/dependency-submission@v4
      with:
        dependency-graph: generate-and-upload
```

***Dependent workflow file***
```yaml
name: Download and submit dependency graph

on:
  workflow_run:
    workflows: ['Generate and save dependency graph']
    types: [completed]

permissions:
  actions: read
  contents: write

jobs:
  submit-dependency-graph:
    runs-on: ubuntu-latest
    steps:
    - name: Download and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
      with:
        dependency-graph: download-and-submit # Download saved dependency-graph and submit
```

# Gradle version compatibility

Dependency-graph generation is compatible with most versions of Gradle >= `5.2`, and is tested regularly against 
Gradle versions `5.2.1`, `5.6.4`, `6.0.1`, `6.9.4`, `7.1.1` and `7.6.3`, as well as all patched versions of Gradle 8.x.

A known exception to this is that Gradle `7.0`, `7.0.1` and `7.0.2` are not supported.

See [here](https://github.com/gradle/github-dependency-graph-gradle-plugin?tab=readme-ov-file#gradle-compatibility) for complete compatibility information.

# Additional references

- Dependency Submission Demo repository: https://github.com/gradle/github-dependency-submission-demo
- GitHub Dependency Graph Gradle Plugin: https://github.com/gradle/github-dependency-graph-gradle-plugin
- Webinar - Gradle at Scale with GitHub and GitHub Actions at Allegro: https://www.youtube.com/watch?v=gV94I28FPos

