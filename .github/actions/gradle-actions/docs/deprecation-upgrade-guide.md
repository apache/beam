# Deprecation upgrade guide

As these actions evolve, certain inputs, behaviour and usages are deprecated for removal.
Deprecated functionality will be fully supported during the current major release, and will be
removed in the next major release.
Users will receive a deprecation warning when they rely on deprecated functionality,
prompting them to update their workflows.

## The action `gradle/gradle-build-action` has been replaced by `gradle/actions/setup-gradle`

The `gradle-build-action` action has evolved, so that the core functionality is now to configure the
Gradle environment for GitHub Actions. For clarity and consistency with other action (eg `setup-java`, `setup-node`), the `gradle-build-action` has been replaced by the `setup-gradle` action.

As of `v3.x`, the `setup-gradle` and `gradle-build-action` actions are functionally identical,
and are released with the same versions.

To convert your workflows, simply replace:
```
   uses: gradle/gradle-build-action@v3
```
with
```
    uses: gradle/actions/setup-gradle@v4
```

## The action `gradle/wrapper-validation-action` has been replaced by `gradle/actions/wrapper-validation`

To facilitate ongoing development, the `wrapper-validation-action` action implementation has been merged into
the https://github.com/gradle/actions repository, and the `gradle/wrapper-validation-action` has been replaced by the `gradle/actions/wrapper-validation` action.

As of `v3.x`, the `gradle/wrapper-validation-action` and `gradle/actions/wrappper-validation` actions are
functionally identical, and are released with the same versions.

In a future major version (likely `v4.x`) we will stop releasing new versions of `gradle/wrapper-validation-action`:
development and releases will continue in the `gradle/actions/wrapper-validation` action.

To convert your workflows, simply replace:
```
   uses: gradle/wrapper-validation-action@v3
```
with
```
    uses: gradle/actions/wrapper-validation@v4
```

## Using the action to execute Gradle via the `arguments` parameter is deprecated

The core functionality of the `setup-gradle` (and `gradle-build-action`) actions is to configure your
Gradle environment for GitHub Actions. Once the action has run, any subsequent Gradle executions will
benefit from caching, reporting and other features of the action.

Using the `arguments` parameter to execute Gradle directly is not necessary to benefit from this action.
This input is deprecated, and will be removed in the `v4` major release of the action.

To convert your workflows, replace any steps using the `arguments` parameter with 2 steps: one to `setup-gradle` and another that runs your Gradle build.

For example, given a workflow like this:

```
steps:
- name: Assemble the project
  uses: gradle/actions/setup-gradle@v3
  with:
    arguments: 'assemble'

 - name: Run the tests
   uses: gradle/actions/setup-gradle@v3
   with:
     arguments: 'test'

 - name: Run build in a subdirectory
   uses: gradle/actions/setup-gradle@v3
   with:
     build-root-directory: another-build
     arguments: 'build'
```

Then replace this with a single call to `setup-gradle` together with separate `run` steps to execute your build.
The exact syntax depends on whether or not your project is configured with the [Gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html).

##### Project uses Gradle wrapper

```
- name: Setup Gradle
  uses: gradle/actions/setup-gradle@v4

- name: Assemble the project
  run: ./gradlew assemble

- name: Run the tests
  run: ./gradlew test

- name: Run build in a subdirectory
  working-directory: another-build
  run: ./gradlew build
```

##### Project doesn't use Gradle wrapper

```
- name: Setup Gradle for a non-wrapper project
  uses: gradle/actions/setup-gradle@v4
  with:
    gradle-version: '8.11'

- name: Assemble the project
  run: gradle assemble

- name: Run the tests
  run: gradle test

- name: Run build in a subdirectory
  working-directory: another-build
  run: gradle build
```

Using the action in this way gives you more control over how Gradle is executed, while still giving you
all of the benefits of the `setup-gradle` action.

The `arguments` parameter is scheduled to be removed in `setup-gradle@v4`.

Note: if you are using the `gradle-build-action`, [see here](#the-action-gradlegradle-build-action-has-been-replaced-by-gradleactionssetup-gradle) for more details on how to migrate.

## The `build-scan-terms-of-service` input parameters have been renamed

With recent releases of the `com.gradle.develocity` plugin, key input parameters have been renamed.
- `build-scan-terms-of-service-url` is now `build-scan-terms-of-use-url`
- `build-scan-terms-of-service-agree` is now `build-scan-terms-of-use-agree`

The standard URL for the terms of use has also changed to https://gradle.com/help/legal-terms-of-use

To convert your workflows, change:
```
    build-scan-publish: true
    build-scan-terms-of-service-url: "https://gradle.com/terms-of-service"
    build-scan-terms-of-service-agree: "yes"
```

to this:
```
    build-scan-publish: true
    build-scan-terms-of-use-url: "https://gradle.com/help/legal-terms-of-use"
    build-scan-terms-of-use-agree: "yes"
```
These deprecated build-scan parameters are scheduled to be removed in `setup-gradle@v4` and `dependency-submission@v4`.

## The GRADLE_ENTERPRISE_ACCESS_KEY env var is deprecated
Gradle Enterprise has been renamed to Develocity starting from Gradle plugin 3.17 and Develocity server 2024.1.
In v4 release of the action, it will require setting the access key with the `develocity-access-key` input and Develocity 2024.1 at least to generate short-lived tokens.
If those requirements are not met, the `GRADLE_ENTERPRISE_ACCESS_KEY` env var will be cleared out and build scan publication or other authenticated Develocity operations won't be possible.

## The `gradle-home-cache-cleanup` input parameter has been replaced by `cache-cleanup`

In versions of the action prior to `v4`, the boolean `gradle-home-cache-cleanup` parameter allows users to opt-in 
to cache cleanup, removing unused files in Gradle User Home prior to saving to the cache.

With `v4`, cache-cleanup is enabled by default, and controlled by the `cache-cleanup` input parameter.

To remove this deprecation:
- If you are using `gradle-home-cache-cleanup: true` in your workflow, you can remove this option as this is now enabled by default.
- If you want cache-cleanup to run even when a Gradle build fails, then add the `cache-cleanup: always` input.
- If cache-cleanup is causing problems with your workflow, you can disable it with `cache-cleanup: never`.
