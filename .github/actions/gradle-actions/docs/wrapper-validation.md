# Gradle Wrapper Validation Action

This action validates the checksums of _all_ [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) JAR files present in the repository and fails if any unknown Gradle Wrapper JAR files are found.

The action should be run in the root of the repository, as it will recursively search for any files named `gradle-wrapper.jar`.

> [!NOTE]
> Starting with v4 the `setup-gradle` action will automatically [perform wrapper validation](../docs/setup-gradle.md#gradle-wrapper-validation)
> on each execution.
> 
> If you are using `setup-gradle` in your workflows, it is unlikely that you will need to use the `wrapper-validation` action.

## The Gradle Wrapper Problem in Open Source

The `gradle-wrapper.jar` is a binary blob of executable code that is checked into nearly
[2.8 Million GitHub Repositories](https://github.com/search?l=&q=filename%3Agradle-wrapper.jar&type=Code).

Searching across GitHub you can find many pull requests (PRs) with helpful titles like 'Update to Gradle xxx'.
Many of these PRs are contributed by individuals outside of the organization maintaining the project.

Many maintainers are incredibly grateful for these kinds of contributions as it takes an item off of their backlog.
We assume that most maintainers do not consider the security implications of accepting the Gradle Wrapper binary from external contributors.
There is a certain amount of blind trust open source maintainers have.
Further compounding the issue is that maintainers are most often greeted in these PRs with a diff to the `gradle-wrapper.jar` that looks like this.

![Image of a GitHub Diff of Gradle Wrapper displaying text 'Binary file not shown.'](https://user-images.githubusercontent.com/1323708/71915219-477d7780-3149-11ea-9254-90c80dbffb0a.png)

A fairly simple social engineering supply chain attack against open source would be contribute a helpful “Updated to Gradle xxx” PR that contains malicious code hidden inside this binary JAR.
A malicious `gradle-wrapper.jar` could execute, download, or install arbitrary code while otherwise behaving like a completely normal `gradle-wrapper.jar`.

## Solution

We have created a simple GitHub Action that can be applied to any GitHub repository.
This GitHub Action will do one simple task:
verify that any and all `gradle-wrapper.jar` files in the repository match the SHA-256 checksums of any of our official releases.

If any are found that do not match the SHA-256 checksums of our official releases, the action will fail.

Additionally, the action will find and SHA-256 hash all
[homoglyph](https://en.wikipedia.org/wiki/Homoglyph)
variants of files named `gradle-wrapper.jar`,
for example a file named `gradlе-wrapper.jar` (which uses a Cyrillic `е` instead of `e`).
The goal is to prevent homoglyph attacks which may be very difficult to spot in a GitHub diff.
We created an example [Homoglyph attack PR here](https://github.com/JLLeitschuh/playframework/pull/1/files).

## Usage

### Add to an existing Workflow

Simply add this action to your workflow **after** having checked out your source tree and **before** running any Gradle build:

```yaml
uses: gradle/actions/wrapper-validation@v4
```

This action step should precede any step using `gradle/gradle-build-action` or `gradle/actions/setup-gradle`.

### Add a new dedicated Workflow

Here's a sample complete workflow you can add to your repositories:

**`.github/workflows/gradle-wrapper-validation.yml`**
```yaml
name: "Validate Gradle Wrapper"

on:
  push:
  pull_request:

jobs:
  validation:
    name: "Validation"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: gradle/actions/wrapper-validation@v4
```

## Contributing to an external GitHub Repository

Since [GitHub Actions](https://github.com/features/actions)
are completely free for open source projects and are automatically enabled on almost all projects,
adding this check to a project's build is as simple as contributing a PR.
Enabling the check requires no overhead on behalf of the project maintainer beyond merging the action.

You can add this action to your favorite Gradle based project without checking out their source locally via the
GitHub Web UI thanks to the 'Create new file' button.

![GitHub 'Create new file' Button bar picture](https://user-images.githubusercontent.com/1323708/73676469-6c023c00-4682-11ea-8c0a-5a1e2d29b17f.png)

Simply add a new file named `.github/workflows/gradle-wrapper-validation.yml` with the contents mentioned above.

We recommend the message commit contents of:
 - Title: `Official Gradle Wrapper Validation Action`
 - Body (at minimum): `See: https://github.com/gradle/actions/wrapper-validation`

From there, you can easily follow the rest of the prompts to create a Pull Request against the project.

## Validation Failures

A wrapper jar can fail validation for a few reasons:
1. The wrapper is from a snapshot build of Gradle (nightly or release nightly) and you have not set `allow-snapshots`
   or `allow-snapshot-wrappers` to `true`.
2. The wrapper jar is from a version of Gradle with an unverifiable wrapper jar (see below).
3. The wrapper jar is saved in Git LFS, and has not been correctly restored on checkout (see below).
4. The wrapper jar was not published by Gradle, and could be compromised.

If this GitHub action fails because a `gradle-wrapper.jar` was not published by Gradle,
we highly recommend that you reach out to us at [security@gradle.com](mailto:security@gradle.com).

#### Unverifiable Wrapper Jars
Wrapper Jars generated by Gradle versions `3.3` to `4.0` are not verifiable because those files were dynamically generated by Gradle in a non-reproducible way. It's not possible to verify the `gradle-wrapper.jar` for those versions are legitimate using a hash comparison. If you have a validation failure, you should try to determine if the `gradle-wrapper.jar` was generated by one of these versions before running the build.

- If the Gradle version in `gradle-wrapper.properties` is outside of this range, you can regenerate the `gradle-wrapper.jar` by running `./gradlew wrapper`. This will generate a new, verifiable wrapper jar.
- If you need to run your build with a version of Gradle between 3.3 and 4.0, you can use a newer version of Gradle to generate the `gradle-wrapper.jar`.

#### Wrapper Jar stored with Git LFS
If your repository is configured to store Wrapper Jars in Git Large File Storage (LFS), then you must include the configuration to correctly
restore these Jars on checkout. Without this, only a pointer to the Wrapper Jar is restored, and the checksum verification will fail.

```
    steps:
      - uses: actions/checkout@v4
        with:
          lfs: true  # gradle-wrapper.jar verification will fail without this
```

## Resources

To learn more about verifying the Gradle Wrapper JAR locally, see our
[guide on the topic](https://docs.gradle.org/current/userguide/gradle_wrapper.html#wrapper_checksum_verification).
