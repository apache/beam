# Configure Gradle for GitHub Actions workflows

This GitHub Action can be used to configure Gradle for optimal execution on any platform supported by GitHub Actions.

## Why use the `setup-gradle` action?

It is possible to directly invoke Gradle in your workflow, and the `actions/setup-java@v4` action provides a simple way to cache Gradle dependencies.

However, the `setup-gradle` action offers a several advantages over this approach:

- Easily [configure your workflow to use a specific version of Gradle](#build-with-a-specific-gradle-version) using the `gradle-version` parameter. Gradle distributions are automatically downloaded and cached.
- More sophisticated and more efficient caching of Gradle User Home between invocations, compared to `setup-java` and most custom configurations using `actions/cache`. [More details below](#caching-build-state-between-jobs).
- Detailed reporting of cache usage and cache configuration options allow you to [optimize the use of the GitHub actions cache](#optimizing-cache-effectiveness).
- [Generate and Submit a GitHub Dependency Graph](#github-dependency-graph-support) for your project, enabling Dependabot security alerts.
- [Automatic capture of Build Scan® links](#build-reporting) from the build, making them easier to locate in workflow runs.

The `setup-gradle` action is designed to provide these benefits with minimal configuration.
These features work both when Gradle is executed via `setup-gradle` and for any Gradle execution in subsequent steps.

## General usage

The `setup-gradle` action works by configuring environment variables and by adding a set of Gradle init-scripts to the Gradle User Home. These will apply to all Gradle executions on the runner, no matter how Gradle is invoked.
This means that if you have an existing workflow that executes Gradle with a `run` step, you can add an initial "Setup Gradle" Step to benefit from caching, build-scan capture, and other features of this action.

The recommended way to execute any Gradle build is with the help of the [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html), and the following examples assume that the Gradle Wrapper has been configured for the project. See [this example](#build-with-a-specific-gradle-version) if your project doesn't use the Gradle Wrapper.


```yaml
name: Run Gradle on every push

on:
  push:

jobs:
  gradle:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4

    - name: Execute Gradle build
      run: ./gradlew build
```

## Build with a specific Gradle version

The `setup-gradle` action can download and install a specified Gradle version, adding this installed version to the PATH.
Downloaded Gradle versions are stored in the GitHub Actions cache, to avoid having to download them again later.

```yaml
 - name: Setup Gradle 8.10
   uses: gradle/actions/setup-gradle@v4
   with:
     gradle-version: '8.10' # Quotes required to prevent YAML converting to number
  - name: Build with Gradle 8.10
    run: gradle build
```

The `gradle-version` parameter can be set to any valid Gradle version.

Moreover, you can use the following aliases:

| Alias | Selects |
| --- |---|
| `wrapper`           | The Gradle wrapper's version (default, useful for matrix builds) |
| `current`           | The current [stable release](https://gradle.org/install/) |
| `release-candidate` | The current [release candidate](https://gradle.org/release-candidate/) if any, otherwise fallback to `current` |
| `nightly`           | The latest [nightly](https://gradle.org/nightly/), fails if none. |
| `release-nightly`   | The latest [release nightly](https://gradle.org/release-nightly/), fails if none.      |

This can be handy to automatically verify your build works with the latest release candidate of Gradle:

The actual Gradle version used is available as an action output: `gradle-version`.

```yaml
name: Test latest Gradle RC
on:
  schedule:
    - cron: 0 0 * * * # daily
jobs:
  gradle-rc:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - uses: gradle/actions/setup-gradle@v4
      id: setup-gradle
      with:
        gradle-version: release-candidate
    - run: gradle build --dry-run # just test build configuration
    - run: echo "The release-candidate version was ${{ steps.setup-gradle.outputs.gradle-version }}"
```

## Caching build state between Jobs

The `setup-gradle` action will use the GitHub Actions cache to save and restore reusable state that may speed up subsequent build invocations. This includes most content that is downloaded from the internet as part of a build, as well as expensive to create content like compiled build scripts, transformed Jar files, etc.

The cached state includes:
- Any distributions downloaded to satisfy a `gradle-version` parameter.
- A subset of the Gradle User Home directory, including downloaded dependencies, wrapper distributions, and the local build cache.

To reduce the space required for caching, this action attempts to reduce duplication in cache entries on a best effort basis.

The state will be restored from the cache during the first `setup-gradle` step for any workflow job, and cache entries will be written back to the cache at the end of the job after all Gradle executions have been completed.

### Disabling caching

Caching is enabled by default. You can disable caching for the action as follows:
```yaml
cache-disabled: true
```

### Using the cache read-only

By default, The `setup-gradle` action will only write to the cache from Jobs on the default (`main`/`master`) branch.
Jobs on other branches will read entries from the cache but will not write updated entries.

This setup is designed around [GitHub imposed restrictions on cache access](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#restrictions-for-accessing-a-cache) and should work well in most scenarios.
See [Optimizing cache effectiveness](#select-which-branches-should-write-to-the-cache) for a more detailed explanation.

In some circumstances, it makes sense to change this default and configure a workflow Job to read existing cache entries but not to write changes back.

You can configure read-only caching for `setup-gradle` as follows:

```yaml
cache-read-only: true
```

You can also configure read-only caching only for certain branches:

```yaml
# Only write to the cache for builds on the 'main' and 'release' branches. (Default is 'main' only.)
# Builds on other branches will only read existing entries from the cache.
cache-read-only: ${{ github.ref != 'refs/heads/main' && github.ref != 'refs/heads/release' }}
```

### Using the cache write-only

In certain circumstances it may be desirable to start with a clean Gradle User Home state, but to save the state at the end of a workflow Job:

```yaml
cache-write-only: true
```

### Configuring cache cleanup

The Gradle User Home directory tends to grow over time. When you switch to a new Gradle wrapper version 
or upgrade a dependency version the old files are not automatically and immediately removed. 
While this can make sense in a local environment, in a GitHub Actions environment
it can lead to ever-larger Gradle User Home cache entries being saved and restored.

To avoid this situation, the `setup-gradle` and `dependency-submission` actions will perform "cache-cleanup", 
purging any unused files from the Gradle User Home before saving it to the GitHub Actions cache. 
Cache cleanup will attempt to remove any files that are initially restored to the Gradle User Home directory 
but that are not used used by Gradle during the GitHub Actions Workflow.

If a Gradle build fails when running the Job, then it is possible that some required files and dependencies 
will not be touched during the Job. To prevent these files from being purged, the default behavior is for 
cache cleanup to run only when all Gradle builds in the Job are successful.

Gradle Home cache cleanup is enabled by default, and can be controlled by the `cache-cleanup` parameter as follows:
- `cache-cleanup: always`: Always run cache cleanup, even when a Gradle build fails in the Job.
- `cache-cleanup: on-success` (default): Run cache cleanup when the Job contains no failing Gradle builds.
- `cache-cleanup: never`: Disable cache cleanup for the Job.

Cache cleanup will never run when the cache is configured as read-only or disabled.

### Overwriting an existing Gradle User Home

When the action detects that the Gradle User Home caches directory already exists (`$GRADLE_USER_HOME/caches`), then by default it will not overwrite the existing content of this directory.
This can occur when a prior action initializes this directory, or when using a self-hosted runner that retains this directory between uses.

In this case, the Job Summary will display a message like:
> Caching for Gradle actions was disabled due to pre-existing Gradle User Home

If you want to override the default and have the caches of the `setup-gradle` action overwrite existing content in the Gradle User Home, you can set the `cache-overwrite-existing` parameter to `true`:

```yaml
cache-overwrite-existing: true
```

### Saving configuration-cache data

When Gradle is executed with the [configuration-cache](https://docs.gradle.org/current/userguide/configuration_cache.html) enabled, the configuration-cache data is stored
in the project directory, at `<project-dir>/.gradle/configuration-cache`. Due to the way the configuration-cache works, [this file may contain stored credentials and other
secrets](https://docs.gradle.org/release-nightly/userguide/configuration_cache.html#config_cache:secrets), and this data needs to be encrypted to be safely stored in the GitHub Actions cache.

> [!IMPORTANT]
> To avoid potentially leaking secrets in the configuration-cache entry, the action will only save or restore configuration-cache data if the `cache-encryption-key` parameter is set.

To benefit from configuration caching in your GitHub Actions workflow, you must:
- Execute your build with Gradle 8.6 or newer. This can be achieved directly or via the Gradle Wrapper.
- Enable the configuration cache for your build.
- Generate a [valid Gradle encryption key](https://docs.gradle.org/8.6/userguide/configuration_cache.html#config_cache:secrets:configuring_encryption_key) and save it as a [GitHub Actions secret](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions).
- Provide the secret key via the `cache-encryption-key` action parameter.

```yaml
jobs:
  gradle-with-configuration-cache:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - uses: gradle/actions/setup-gradle@v4
      with:
        gradle-version: '8.6'
        cache-encryption-key: ${{ secrets.GRADLE_ENCRYPTION_KEY }}
    - run: gradle build --configuration-cache
```

Even with everything correctly configured, you may find that the configuration-cache entry is not reused in your workflow.
This is often due to a known issue: [Included builds containing build logic prevent configuration-cache reuse](https://github.com/gradle/actions/issues/21). Refer to the issue for more details.

> [!NOTE]
> The configuration cache cannot be saved or restored in workflows triggered by a pull requests from a repository fork.
> This is because [GitHub secrets are not passed to workflows triggered by PRs from forks](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions#using-secrets-in-a-workflow).
> This prevents a malicious PR from reading the configuration-cache data, which may encode secrets read by Gradle.

### Incompatibility with other caching mechanisms

When using `setup-gradle` we recommend that you avoid using other mechanisms to save and restore the Gradle User Home.

Specifically:
- Avoid using `actions/cache` configured to cache the Gradle User Home, [as described in this example](https://github.com/actions/cache/blob/main/examples.md#java---gradle).
- Avoid using `actions/setup-java` with the `cache: gradle` option, [as described here](https://github.com/actions/setup-java#caching-gradle-dependencies).

Using either of these mechanisms may interfere with the caching provided by this action. If you choose to use a different mechanism to save and restore the Gradle User Home, you should disable the caching provided by this action, as described above.

## How Gradle User Home caching works

### Properties of the GitHub Actions cache

The GitHub Actions cache has some properties that present problems for efficient caching of the Gradle User Home.
- Immutable entries: once a cache entry is written for a key, it cannot be overwritten or changed.
- Branch scope: cache entries written for a Git branch are not visible from actions running against different branches or tags. Entries written for the default branch are visible to all. https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#restrictions-for-accessing-a-cache
- Restore keys: if no exact match is found, a set of partial keys can be provided that will match by cache key prefix. https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows#matching-a-cache-key

Each of these properties has influenced the design and implementation of the caching in `setup-gradle`, as described below.

### Which content is cached

Using experiments and observations, we have attempted to identify which Gradle User Home content is worth saving and restoring between build invocations. We considered both the respective size of the content and the impact this content has on build times. As well as the obvious candidates like downloaded dependencies, we saw that compiled build scripts, transformed Jar files, and other content can also have a significant impact.

In the end, we opted to save and restore as much content as is practical, including:
- `caches/<version>/generated-gradle-jars`: These files are generated on the first use of a particular Gradle version, and are expensive to recreate
- `caches/<version>/kotlin-dsl` and `caches/<version>/scripts`: These are the compiled build scripts. The Kotlin ones in particular can benefit from caching.
- `caches/modules-2`: The downloaded dependencies
- `caches/transforms-3`: The results of artifact transforms
- `caches/jars-9`: Jar files that have been processed/instrumented by Gradle
- `caches/build-cache-1`: The local build cache

In certain cases, a particular section of Gradle User Home will be too large to make caching effective. In these cases, particular subdirectories can be excluded from caching. See [Exclude content from Gradle User Home cache](#exclude-content-from-gradle-user-home-cache).

### Cache keys

The actual content of the Gradle User Home after a build is the result of many factors, including:
- Core Gradle build files (`settings.gradle[.kts]`, `build.gradle[.kts]`, `gradle.properties`)
- Associated Gradle configuration files (`gradle-wrapper.properties`, `dependencies.toml`, etc)
- The entire content of `buildSrc` or any included builds that provide plugins.
- The entire content of the repository, in the case of the local build cache.
- The actual build command that was invoked, including system properties and environment variables.

For this reason, it's very difficult to create a cache key that will deterministically map to a saved Gradle User Home state. So instead of trying to reliably hash all of these inputs to generate a cache key, the Gradle User Home cache key is based on the currently executing Job and the current commit hash for the repository.

The Gradle User Home cache key is composed of:
- The current operating system (`RUNNER_OS`)
- The Job id
- A hash of the Job matrix parameters and the workflow name
- The git SHA for the latest commit

Specifically, the cache key is: `${cache-protocol}-gradle|${runner-os}|${job-id}[${hash-of-job-matrix-and-workflow-name}]-${git-sha}`

As such, the cache key is likely to change on each subsequent run of GitHub actions.
This allows the most recent state to always be available in the GitHub actions cache.

### Finding a matching cache entry

In most cases, no exact match will exist for the cache key. Instead, the Gradle User Home will be restored for the closest matching cache entry, using a set of "restore keys". The entries will be matched with the following precedence:
- An exact match on OS, job id, workflow name, matrix, and Git SHA
- The most recent entry saved for the same OS, job id, workflow name, and matrix values
- The most recent entry saved for the same OS and job id
- The most recent entry saved for the same OS

Due to branch scoping of cache entries, the above match will be first performed for entries from the same branch, and then for the default ('main') branch.

After the Job is complete, the current Gradle User Home state will be collected and written as a new cache entry with the complete cache key. Old entries will be expunged from the GitHub Actions cache on a least recently used basis.

Note that while effective, this mechanism is not inherently efficient. It requires the entire Gradle User Home directory to be stored separately for each branch, for every OS+Job+Matrix combination. In addition, it writes a new cache entry on every GitHub Actions run.

This inefficiency is effectively mitigated by [Deduplication of Gradle User Home cache entries](#deduplication-of-gradle-user-home-cache-entries) and can be further optimized for a workflow using the techniques described in [Optimizing cache effectiveness](#optimizing-cache-effectiveness).

### Deduplication of Gradle User Home cache entries

To reduce duplication between cache entries, certain artifacts in Gradle User Home are extracted and cached independently based on their identity. This allows each Gradle User Home cache entry to be relatively small, sharing common elements between them without duplication.

Artifacts that are cached independently include:
- Downloaded dependencies
- Downloaded wrapper distributions
- Generated Gradle API jars
- Downloaded Java Toolchains

For example, this means that all jobs executing a particular version of the Gradle wrapper will share a single common entry for this wrapper distribution and one for each of the generated Gradle API jars.

### Stopping the Gradle daemon

By default, the action will stop all running Gradle daemons in the post-action step, before saving the Gradle User Home state.
This allows for any Gradle User Home cleanup to occur, and avoid file-locking issues on Windows.

If caching is disabled or the cache is in read-only mode, the daemon will not be stopped and will continue running after the job is completed.

## Optimizing cache effectiveness

Cache storage space for GitHub actions is limited, and writing new cache entries can trigger the deletion of existing entries.
Eviction of shared cache entries can reduce cache effectiveness, slowing down your `setup-gradle` steps.

There are a several actions you can take if your cache use is less effective due to entry eviction.

At the end of a Job, The `setup-gradle` action will write a summary of the Gradle builds executed, together with a detailed report of the cache entries that were read and written during the Job. This report can provide valuable insights that may help to determine the right way to optimize the cache usage for your workflow.

### Select which jobs should write to the cache

Consider a workflow that first runs a Job "compile-and-unit-test" to compile the code and run some basic unit tests, which is followed by a matrix of parallel "integration-test" jobs that each run a set of integration tests for the repository. Each "integration test" Job requires all of the dependencies required by "compile-and-unit-test", and possibly one or 2 additional dependencies.

By default, a new cache entry will be written on completion of each integration test job. If no additional dependencies were downloaded then this cache entry will share the "dependencies" entry with the "compile-and-unit-test" job, but if a single dependency was downloaded then an entirely new "dependencies" entry would be written. (The `setup-gradle` action does not _yet_ support a layered cache that could do this more efficiently). If each of these "integration-test" entries with their different "dependencies" entries is too large, then it could result in other important entries being evicted from the GitHub Actions cache.

Some techniques can be used to avoid/mitigate this issue:
- Configure the "integration-test" jobs with `cache-read-only: true`, meaning that the Job will use the entry written by the "compile-and-unit-test" job. This will avoid the overhead of cache entries for each of these jobs, at the expense of re-downloading any additional dependencies required by "integration-test".
- Add a step to the "compile-and-unit-test" job which downloads all dependencies required by the integration-test jobs but does not execute the tests. This will allow the "dependencies" entry for "compile-and-unit-test" to be shared among all cache entries for "integration-test". The resulting "integration-test" entries should be much smaller, reducing the potential for eviction.
- Combine the above 2 techniques, so that no cache entry is written by "integration-test" jobs, but all required dependencies are already present from the restored "compile-and-unit-test" entry.

### Select which branches should write to the cache

GitHub cache entries are not shared between builds on different branches or tags.
Workflow runs can _only_ restore caches created in either the same branch or the default branch (usually `main`).
This means that each branch will have its own Gradle User Home cache scope, and will not benefit from cache entries written for other (non-default) branches.

By default, The `setup-gradle` action will only _write_ to the cache for builds run on the default (`master`/`main`) branch.
Jobs running on other branches will only read from the cache. In most cases, this is the desired behavior.
This is because Jobs running on other branches will benefit from the cached Gradle User Home from `main`,
without writing private cache entries which could lead to evicting these shared entries.

If you have other long-lived development branches that would benefit from writing to the cache,
you can configure this by disabling the `cache-read-only` action parameter for these branches.
See [Using the cache read-only](#using-the-cache-read-only) for more details.

Note there are some cases where writing cache entries is typically unhelpful (these are disabled by default):
- For `pull_request` triggered runs, the cache scope is limited to the merge ref (`refs/pull/.../merge`) and can only be restored by re-runs of the same pull request.
- For `merge_group` triggered runs, the cache scope is limited to a temporary branch with a special prefix created to validate pull request changes, and won't be available on subsequent Merge Queue executions.

### Exclude content from Gradle User Home cache

As well as any wrapper distributions, the action will attempt to save and restore the `caches` and `notifications` directories from Gradle User Home.

Each build is different, and some builds produce more Gradle User Home content than others.
[Cache debugging ](#cache-debugging-and-analysis) can provide insight into which cache entries are the largest,
and the contents to be cached can be fine-tuned by including and excluding certain paths within the Gradle User Home.

```yaml
# Cache downloaded JDKs in addition to the default directories.
gradle-home-cache-includes: |
    caches
    notifications
    jdks
# Exclude the local build-cache and keyrings from the directories cached.
gradle-home-cache-excludes: |
    caches/build-cache-1
    caches/keyrings
```

You can specify any number of fixed paths or patterns to include or exclude.
File pattern support is documented at https://docs.github.com/en/actions/learn-github-actions/workflow-syntax-for-github-actions#patterns-to-match-file-paths.

### Disable local build-cache when remote build-cache is available

If you have a remote build-cache available for your build, then it is recommended to do the following:
- Enable [remote build-cache push](https://docs.gradle.org/current/userguide/build_cache.html#sec:build_cache_configure_use_cases) for your GitHub Actions builds
- Disable [local build-cache]() for your GitHub Actions build

As well as reducing the content that needs to be saved to the GitHub Actions cache,
this setup will ensure that your CI builds populate the remote cache and keep the cache entries fresh by reading these entries.
Local builds can then benefit from the remote cache.

## Debugging and Troubleshooting

To debug a failed job, it can be useful to run with [debug logging enabled](https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/enabling-debug-logging).
You can enable debug logging either by:
1. Adding an `ACTIONS_STEP_DEBUG` variable to your repository configuration ([see here](https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/enabling-debug-logging#enabling-step-debug-logging)).
2. By re-running a Job and checking the "Enable debug logging" box ([see here](https://github.blog/changelog/2022-05-24-github-actions-re-run-jobs-with-debug-logging/)).

### Increased logging from Gradle builds

When debug logging is enabled, this action will cause all builds to run with the `--info` and `--stacktrace` options.
This is done by inserting the relevant [Gradle properties](https://docs.gradle.org/current/userguide/build_environment.html#sec:gradle_configuration_properties)
at the top of the `${GRADLE_USER_HOME}/gradle.properties` file.

If the additional Gradle logging produced is problematic, you may opt out of this behavior by setting these properties manually in your project `gradle.properties` file:

```properties
# default lifecycle
org.gradle.logging.level=lifecycle
org.gradle.logging.stacktrace=internal
```

### Cache debugging and analysis

A report of all cache entries restored and saved is printed to the Job Summary when saving the cache entries.
This report can provide valuable insight into how much cache space is being used.

When debug logging is enabled, more detailed logging of cache operations is included in the GitHub actions log.
This includes a breakdown of the contents of the Gradle User Home directory, which may assist in cache optimization.

## Build reporting

The `setup-gradle` action collects information about any Gradle executions that occur in a workflow, including the root project,
requested tasks, build outcome, and any Build Scan link generated. Details of cache entries read and written are also collected.
These details are compiled into a Job Summary, which is visible in the GitHub Actions UI.

Generation of a Job Summary is enabled by default for all Jobs using The `setup-gradle` action. This feature can be configured
so that a Job Summary is never generated, or so that a Job Summary is only generated on build failure:
```yaml
add-job-summary: 'on-failure' # Valid values are 'always' (default), 'never', and 'on-failure'
```

### Excluding specific Gradle builds from Job Summary

The Job Summary works by installing an init-script in Gradle User Home which will record details of any Gradle execution during the workflow.
This means that any Gradle excecution sharing the same Gradle User Home will show up in the Job Summary, which may include Gradle executions 
run as part of integration testing.

To avoid having these test builds show up in the Job Summary, add the `GRADLE_ACTIONS_SKIP_BUILD_RESULT_CAPTURE=true` environment variable
to the process that executes Gradle. This will stop the init-script from collecting any build results.

### Adding Job Summary as a Pull Request comment

It is sometimes more convenient to view the results of a GitHub Actions Job directly from the Pull Request that triggered
the Job. For this purpose, you can configure the action so that Job Summary data is added as a Pull Request comment.


```yaml
name: CI
on:
  pull_request:

permissions:
  pull-requests: write

jobs:
  run-gradle-build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        add-job-summary-as-pr-comment: 'on-failure' # Valid values are 'never' (default), 'always', and 'on-failure'

    - run: ./gradlew build --scan
```

Note that to add a Pull Request comment, the workflow must be configured with the `pull-requests: write` permission.


### Build Scan® link as Step output

As well as reporting all [Build Scan](https://gradle.com/build-scans/) links in the Job Summary,
The `setup-gradle` action makes this link available as an output of any Step that executes Gradle.

The output name is `build-scan-url`. You can then use the build scan link in subsequent actions of your workflow.

### Saving arbitrary build outputs

By default, a GitHub Actions workflow using `setup-gradle` will record the log output and any Build Scan
links for your build, but any output files generated by the build will not be saved.

To save selected files from your build execution, you can use the core [Upload-Artifact](https://github.com/actions/upload-artifact) action.
For example:

```yaml
jobs:
  gradle:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4

    - name: Run build with Gradle wrapper
      run: ./gradlew build --scan

    - name: Upload build reports
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: build-reports
        path: **/build/reports/
```

### Use of custom init-scripts in Gradle User Home

Note that the action collects information about Gradle invocations via an [Initialization Script](https://docs.gradle.org/current/userguide/init_scripts.html#sec:using_an_init_script)
located at `USER_HOME/.gradle/init.d/gradle-actions.build-result-capture.init.gradle`.

If you are adding any custom init scripts to the `USER_HOME/.gradle/init.d` directory, it may be necessary to ensure these files are applied before `gradle-actions.build-result-capture.init.gradle`.
Since Gradle applies init scripts in alphabetical order, one way to ensure this is via file naming.

## Gradle Wrapper validation

By default, this action will perform the same wrapper validation as is performed by the dedicated 
[wrapper-validation action](./wrapper-validation.md). 
This means that invalid wrapper jars will be automatically detected when using `setup-gradle`. 

If you do not want wrapper-validation to occur automatically, you can disable it:

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        validate-wrappers: false
```

If your repository uses snapshot versions of the Gradle wrapper, such as nightly builds, then you'll need to 
explicitly allow snapshot wrappers in wrapper validation.
These are not allowed by default.


```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        validate-wrappers: true
        allow-snapshot-wrappers: true
```

If you need more advanced configuration, then you're advised to continue using a separate workflow step
with `gradle/actions/wrapper-validation`.

## Support for GitHub Enterprise Server (GHES)

You can use the `setup-gradle` action on GitHub Enterprise Server, and benefit from the improved integration with Gradle. Depending on the version of GHES you are running, certain features may be limited:
- Build Scan links are captured and displayed in the GitHub Actions UI
- Easily run your build with different versions of Gradle
- Save/restore of Gradle User Home (requires GHES v3.5+ : GitHub Actions cache was introduced in GHES 3.5)
- Support for GitHub Actions Job Summary (requires GHES 3.6+ : GitHub Actions Job Summary support was introduced in GHES 3.6). In earlier versions of GHES, the build-results summary and caching report will be written to the workflow log, as part of the post-action step.

## GitHub Dependency Graph support

> [!IMPORTANT]
> The simplest (and recommended) way to generate a dependency graph is via a separate workflow
> using `gradle/actions/dependency-submission`. This action will attempt to detect all dependencies used by your build
> without building and testing the project itself.
>
> See the [dependency-submission documentation](dependency-submission.md) for up-to-date documentation.


The `setup-gradle` action has support for submitting a [GitHub Dependency Graph](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/about-the-dependency-graph) snapshot via the [GitHub Dependency Submission API](https://docs.github.com/en/rest/dependency-graph/dependency-submission?apiVersion=2022-11-28).

The dependency graph snapshot is generated via integration with the [GitHub Dependency Graph Gradle Plugin](https://plugins.gradle.org/plugin/org.gradle.github-dependency-graph-gradle-plugin) and saved as a workflow artifact. The generated snapshot files can be submitted either in the same job or in a subsequent job (in the same or a dependent workflow).

The generated dependency graph snapshot reports all of the dependencies that were resolved during a build execution, and is used by GitHub to generate [Dependabot Alerts](https://docs.github.com/en/code-security/dependabot/dependabot-alerts/about-dependabot-alerts) for vulnerable dependencies, as well as to populate the [Dependency Graph insights view](https://docs.github.com/en/code-security/supply-chain-security/understanding-your-software-supply-chain/exploring-the-dependencies-of-a-repository#viewing-the-dependency-graph).

### Basic usage

You enable GitHub Dependency Graph support by setting the `dependency-graph` action parameter. Valid values are:

| Option | Behaviour |
| --- | --- |
| `disabled`            | Do not generate a dependency graph for any build invocations.<p>This is the default. |
| `generate`            | Generate a dependency graph snapshot for each build invocation. |
| `generate-and-submit` | Generate a dependency graph snapshot for each build invocation, and submit these via the Dependency Submission API on completion of the job. |
| `generate-and-upload` | Generate a dependency graph snapshot for each build invocation, saving it as a workflow artifact. |
| `download-and-submit` | Download any previously saved dependency graph snapshots, and submit them via the Dependency Submission API. This can be useful to submit [dependency graphs for pull requests submitted from repository forks](dependency-submission.md#usage-with-pull-requests-from-public-forked-repositories). |

Example of a CI workflow that generates and submits a dependency graph:
```yaml
name: CI build
on:
  push:

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle to generate and submit dependency graphs
      uses: gradle/actions/setup-gradle@v4
      with:
        dependency-graph: generate-and-submit
    - name: Run the usual CI build (dependency-graph will be generated and submitted post-job)
      run: ./gradlew build
```

The `contents: write` permission is required to submit (but not generate) the dependency graph file.
Depending on [repository settings](https://docs.github.com/en/actions/security-guides/automatic-token-authentication#permissions-for-the-github_token), this permission may be available by default or may need to be explicitly enabled in the workflow file (as above).

> [!IMPORTANT]
> The above configuration will work for workflows that run as a result of commits to a repository branch,
> but not when a workflow is triggered by a PR from a repository fork.
> This is because the `contents: write` permission is not available when executing a workflow
> for a PR submitted from a forked repository.
> For a configuration that supports this setup, see [Dependency Graphs for pull request workflows](dependency-submission.md#usage-with-pull-requests-from-public-forked-repositories).

### Making dependency graph failures cause Job failures

By default, if a failure is encountered when generating or submitting the dependency graph, the action will log the failure as a warning and continue.
This allows your workflow to be resilient to dependency graph failures, in case dependency graph production is a side-effect rather than the primary purpose of a workflow.

If instead, you have a workflow whose primary purpose is to generate and submit a dependency graph, it makes sense for this workflow to fail if the dependency
graph cannot be generated or submitted. You can enable this behavior with the `dependency-graph-continue-on-failure` parameter, which defaults to `true`.

```yaml
# Ensure that the workflow Job will fail if the dependency graph cannot be submitted
- uses: gradle/actions/setup-gradle@v4
  with:
    dependency-graph: generate-and-submit
    dependency-graph-continue-on-failure: false
```

### Using a custom plugin repository

By default, the action downloads the `github-dependency-graph-gradle-plugin` from the Gradle Plugin Portal (https://plugins.gradle.org). If your GitHub Actions environment does not have access to this URL, you can specify a custom plugin repository to use.

Do so by setting the `GRADLE_PLUGIN_REPOSITORY_URL` environment variable with your Gradle invocation.
The `GRADLE_PLUGIN_REPOSITORY_USERNAME` and `GRADLE_PLUGIN_REPOSITORY_PASSWORD` can be used when the plugin repository requires authentication.

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle to generate and submit dependency graphs
      uses: gradle/actions/setup-gradle@v4
      with:
        dependency-graph: generate-and-submit
    - name: Run a build, resolving the 'dependency-graph' plugin from the plugin portal proxy
      run: ./gradlew build
      env:
        GRADLE_PLUGIN_REPOSITORY_URL: 'https://gradle-plugins-proxy.mycorp.com'

        # Set the following variables if your custom plugin repository requires authentication
        # GRADLE_PLUGIN_REPOSITORY_USERNAME: "username"
        # GRADLE_PLUGIN_REPOSITORY_PASSWORD: ${secrets.MY_REPOSITORY_PASSWORD}
```

### Choosing which Gradle invocations will generate a dependency graph

Once you enable the dependency graph support for a workflow job (via the `dependency-graph` parameter), dependencies will be collected and reported for all subsequent Gradle invocations.
If you have a Gradle build step that you want to exclude from dependency graph generation, you can set the `GITHUB_DEPENDENCY_GRAPH_ENABLED` environment variable to `false`.

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-java@v4
      with:
        distribution: temurin
        java-version: 17

    - name: Setup Gradle to generate and submit dependency graphs
      uses: gradle/actions/setup-gradle@v4
      with:
        dependency-graph: generate-and-submit
    - name: Build the app, generating a graph of dependencies required
      run: ./gradlew :my-app:assemble
    - name: Run all checks, disabling dependency graph generation
      run: ./gradlew check
      env:
        GITHUB_DEPENDENCY_GRAPH_ENABLED: false
```

### Filtering which Gradle Configurations contribute to the dependency graph

If you do not want the dependency graph to include every dependency configuration in every project in your build,
you can limit the dependency extraction to a subset of these.

See the documentation for [dependency-submission](dependency-submission.md) and the
[GitHub Dependency Graph Gradle Plugin](https://github.com/gradle/github-dependency-graph-gradle-plugin?tab=readme-ov-file#filtering-which-gradle-configurations-contribute-to-the-dependency-graph) for details.

### Gradle version compatibility

Dependency-graph generation is compatible with most versions of Gradle >= `5.2`, and is tested regularly against
Gradle versions `5.2.1`, `5.6.4`, `6.0.1`, `6.9.4`, `7.1.1` and `7.6.3`, as well as all patched versions of Gradle 8.x.

A known exception to this is that Gradle `7.0`, `7.0.1`, and `7.0.2` are not supported.

See [here](https://github.com/gradle/github-dependency-graph-gradle-plugin?tab=readme-ov-file#gradle-compatibility) for complete compatibility information.

# Develocity Build Scan® integration

Publishing a Develocity Build Scan can be very helpful for Gradle builds run on GitHub Actions. Each Build Scan provides a
detailed report of the execution of the build, including which tasks were executed and the results of any test execution.

The `setup-gradle` plugin provides a number of features to enable and enhance publishing Build Scans® to a Develocity instance.

## Publishing to scans.gradle.com

If you don't have a a private Develocity instance, you can easily publish Build Scans to the 
free, public Develocity instance (https://scans.gradle.com).

To publish to https://scans.gradle.com, you must specify in your workflow that you accept the [Gradle Terms of Use](https://gradle.com/help/legal-terms-of-use).

```yaml
    - name: Setup Gradle to publish build scans
      uses: gradle/actions/setup-gradle@v4
      with:
        build-scan-publish: true
        build-scan-terms-of-use-url: 'https://gradle.com/terms-of-service'
        build-scan-terms-of-use-agree: 'yes'

    - name: Run a Gradle build - a build scan will be published automatically
      run: ./gradlew build
```

If your build is configured to [publish on demand](https://docs.gradle.com/develocity/gradle-plugin/current/#publishing_on_demand) 
using `onlyIf { false }`, setting `build-scan-publish: true` will not force a scan to be published.

## Managing Develocity access keys

Develocity access keys are long-lived, creating risks if they are leaked. To mitigate this risk this, 
the `setup-gradle` action can automatically attempt to obtain a [short-lived access token](https://docs.gradle.com/develocity/gradle-plugin/current/#short_lived_access_tokens)
to use when authenticating with Develocity. 
The short-lived access token will then be used wherever a Develocity access key is required.

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        develocity-access-key: ${{ secrets.MY_DEVELOCITY_ACCESS_KEY }} # Long-lived access key, visiblility is restricted to this step.

    # Subsequent steps will automatically use a short-lived access token to authenticate with Develocity
    - name: Run a Gradle build that is configured to publish to Develocity.
      run: ./gradlew build
```

### Increasing the expiry time for Develocity access tokens

By default, a short-lived Develocity access token will be valid for 2 hours from the time it is generated. If your workflows take longer than
2 hours to complete, you may see failure to publish Build Scans due to access token expiry.

To avoid this, use the `develocity-token-expiry` parameter to specify a different token expiry in hours.

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        develocity-access-key: ${{ secrets.MY_DEVELOCITY_ACCESS_KEY }}
        develocity-token-expiry: '8' # The number of hours that the access token should remain valid (max 24).
```

### Develocity access key supplied as environment variable

The preferred mechanism is to supply the long-lived Develocity access key directly to `setup-gradle` via 
the `develocity-access-key` input variable. However, the action will also detect an access key configured as an environment variable,
such as `DEVELOCITY_ACCESS_KEY` or `GRADLE_ENTERPRISE_ACCESS_KEY`. In this case, the environment variable value will be replaced by 
a short-lived access token, thus hiding the long-lived access key from subsequent steps.

```yaml
env:
  DEVELOCITY_ACCESS_KEY: ${{ secrets.MY_DEVELOCITY_ACCESS_KEY }}

jobs:
  build-with-gradle:
    runs-on: ubuntu-latest
    steps:
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4

    # The build will automatically use a short-lived access token to authenticate with Develocity
    - name: Run a Gradle build that is configured to publish to Develocity.
      run: ./gradlew build
```

### Failure to obtain a short-lived access token

If a short-lived token cannot be retrieved (for example, if the Develocity server version is lower than `2024.1`):
 - If the access key is provided via `develocity-access-key`, then no access token is set and authentication with Develocity will not succeed.
 - If the access key is provided via an environment variable, a warning will be logged and the environment variable will be left as-is. 
   This can result in long-lived access keys being unintentionally exposed to other workflow steps.
For more information on short-lived tokens, see [Develocity API documentation](https://docs.gradle.com/develocity/api-manual/#short_lived_access_tokens).

## Develocity plugin injection

The `setup-gradle` action provides support for transparently injecting and configuring the Develocity Gradle plugin into any Gradle build, 
without any modification to the project sources. This allows Build Scans to be published for a repository without any changes to the project sources.

Develocity injection is achieved via an init-script installed into Gradle User Home, which is enabled and parameterized via environment variables.

The same auto-injection behavior is available for the Common Custom User Data Gradle plugin, which enriches any build scans published with additional useful information.

### Enabling Develocity injection

To enable Develocity injection for your build, you must provide the required configuration via inputs.

Here's a minimal example:

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        develocity-injection-enabled: true
        develocity-url: 'https://develocity.your-server.com'
        develocity-plugin-version: '4.0'

    - name: Run a Gradle build with Develocity injection enabled
      run: ./gradlew build
```

This configuration will automatically apply `v4.2` of the [Develocity Gradle plugin](https://docs.gradle.com/develocity/gradle-plugin/), and publish build scans to https://develocity.your-server.com.

This example assumes that the `develocity.your-server.com` server allows anonymous publishing of build scans.
In the likely scenario that your Develocity server requires authentication, you will also need to pass a valid [Develocity access key](https://docs.gradle.com/develocity/gradle-plugin/#via_environment_variable) taken from a secret:

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4
      with:
        develocity-access-key: ${{ secrets.MY_DEVELOCITY_ACCESS_KEY }}

    - name: Run a Gradle build with Develocity injection enabled
      run: ./gradlew build
      with:
        develocity-injection-enabled: true
        develocity-url: 'https://develocity.your-server.com'
        develocity-plugin-version: '4.0'
```

This access key will be used during the action execution to get a short-lived token and set it to the DEVELOCITY_ACCESS_KEY environment variable.

### Configuring Develocity injection

The `init-script` supports several additional configuration parameters that you may find useful. All configuration options (required and optional) are detailed below:

| Variable                             | Required | Description                                                                                                                                                             |
|--------------------------------------| :---: |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| develocity-injection-enabled         | :white_check_mark: | enables Develocity injection                                                                                                                                            |
| develocity-url                       | :white_check_mark: | the URL of the Develocity server                                                                                                                                        |
| develocity-allow-untrusted-server    | | allow communication with an untrusted server; set to _true_ if your Develocity instance is using a self-signed certificate                                              |
| develocity-capture-file-fingerprints | | enables capturing the paths and content hashes of each individual input file                                                                                            |
| develocity-enforce-url               | | enforce the configured Develocity URL over a URL configured in the project's build; set to _true_ to enforce publication of build scans to the configured Develocity URL |
| develocity-plugin-version            | :white_check_mark: | the version of the [Develocity Gradle plugin](https://docs.gradle.com/develocity/gradle-plugin/) to apply                                                               |
| develocity-ccud-plugin-version       |  | the version of the [Common Custom User Data Gradle plugin](https://github.com/gradle/common-custom-user-data-gradle-plugin) to apply, if any                            |
| gradle-plugin-repository-url         |  | the URL of the repository to use when resolving the Develocity and CCUD plugins; the Gradle Plugin Portal is used by default                                            |
| gradle-plugin-repository-username    |  | the username for the repository URL to use when resolving the Develocity and CCUD plugins                                                                               |
| gradle-plugin-repository-password    |  | the password for the repository URL to use when resolving the Develocity and CCUD plugins; Consider using secrets to pass the value to this variable                    |

The input parameters can be expressed as environment variables following the relationships outlined in the table below:

| Input                                | Environment Variable                           |
|--------------------------------------|------------------------------------------------|
| develocity-injection-enabled         | DEVELOCITY_INJECTION_ENABLED                   |
| develocity-url                       | DEVELOCITY_INJECTION_URL                       |
| develocity-enforce-url               | DEVELOCITY_INJECTION_ENFORCE_URL               |
| develocity-allow-untrusted-server    | DEVELOCITY_INJECTION_ALLOW_UNTRUSTED_SERVER    |
| develocity-capture-file-fingerprints | DEVELOCITY_INJECTION_CAPTURE_FILE_FINGERPRINTS |
| develocity-plugin-version            | DEVELOCITY_INJECTION_DEVELOCITY_PLUGIN_VERSION |
| develocity-ccud-plugin-version       | DEVELOCITY_INJECTION_CCUD_PLUGIN_VERSION       |
| gradle-plugin-repository-url         | DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_URL     |
| gradle-plugin-repository-username    | DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_USERNAME|
| gradle-plugin-repository-password    | DEVELOCITY_INJECTION_PLUGIN_REPOSITORY_PASSWORD|


Here's an example using the env vars:

```yaml
    - name: Setup Gradle
      uses: gradle/actions/setup-gradle@v4

    - name: Run a Gradle build with Develocity injection enabled with environment variables
      run: ./gradlew build
      env:
        DEVELOCITY_INJECTION_ENABLED: true
        DEVELOCITY_INJECTION_URL: https://develocity.your-server.com
        DEVELOCITY_INJECTION_ENFORCE_URL: true
        DEVELOCITY_INJECTION_DEVELOCITY_PLUGIN_VERSION: '4.0'
        DEVELOCITY_INJECTION_CCUD_PLUGIN_VERSION: '2.2.1'
```

# Dependency verification

Develocity injection, Build Scan publishing and Dependency Graph generation all work by applying external plugins to your build.
If you project has [dependency verification enabled](https://docs.gradle.org/current/userguide/dependency_verification.html#sec:signature-verification), 
then you'll need to update your verification metadata to trust these plugins.

Each of the plugins is signed by Gradle, and you can simply add the following snippet to your `dependency-verificaton.xml` file:

```xml
<trusted-keys>
   <trusted-key id="7B79ADD11F8A779FE90FD3D0893A028475557671">
      <trusting group="com.gradle"/>
      <trusting group="org.gradle"/>
   </trusted-key>
</trusted-keys>
```

