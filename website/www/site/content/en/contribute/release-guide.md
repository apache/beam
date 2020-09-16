---
title: "Beam Release Guide"
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

# Apache Beam Release Guide

{{< toc >}}

## Introduction

The Apache Beam project periodically declares and publishes releases. A release is one or more packages of the project artifact(s) that are approved for general public distribution and use. They may come with various degrees of caveat regarding their perceived quality and potential for change, such as “alpha”, “beta”, “incubating”, “stable”, etc.

The Beam community treats releases with great importance. They are a public face of the project and most users interact with the project only through the releases. Releases are signed off by the entire Beam community in a public vote.

Each release is executed by a *Release Manager*, who is selected among the Beam committers. This document describes the process that the Release Manager follows to perform a release. Any changes to this process should be discussed and adopted on the [dev@ mailing list](/get-started/support/).

Please remember that publishing software has legal consequences. This guide complements the foundation-wide [Product Release Policy](http://www.apache.org/dev/release.html) and [Release Distribution Policy](http://www.apache.org/dev/release-distribution).

### Overview

<img src="/images/release-guide-1.png" alt="Alt text" width="100%">

The release process consists of several steps:

1. Decide to release
1. Prepare for the release
1. Investigate performance regressions
1. Create a release branch
1. Verify release branch
1. Build a release candidate
1. Vote on the release candidate
1. During vote process, run validation tests
1. If necessary, fix any issues and go back to step 3.
1. Finalize the release
1. Promote the release


## 1. Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based decision of the entire community.

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any objections should be resolved by consensus before starting the release.

In general, the community prefers to have a rotating set of 3-5 Release Managers. Keeping a small core set of managers allows enough people to build expertise in this area and improve processes over time, without Release Managers needing to re-learn the processes for each release. That said, if you are a committer interested in serving the community in this way, please reach out to the community on the dev@ mailing list.

### Checklist to proceed to the next step

1. Community agrees to release
1. Community selects a Release Manager

**********

## 2. Prepare for the release

Before your first release, you should perform one-time configuration steps. This will set up your security keys for signing the release and access to various release repositories.

To prepare for each release, you should audit the project status in the JIRA issue tracker, and do necessary bookkeeping. Finally, you should create a release branch from which individual release candidates will be built.

__NOTE__: If you are using [GitHub two-factor authentication](https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/) and haven't configure HTTPS access,
please follow [the guide](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) to configure command line access.


### Accounts

Please have these credentials ready at hand, you will likely need to enter them multiple times:

* GPG pass phrase (see the next section);
* Apache ID and Password;
* GitHub ID and Password.
* DockerHub ID and Password. (You should be a member of maintainer team; email at dev@ if you are not.)


### One-time setup instructions


#### GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don’t have a GPG key associated with your Apache account, please create one according to the guidelines.

There are 2 ways to configure your GPG key for release, either using release automation script(which is recommended),
or running all commands manually.

##### Use preparation_before_release.sh to setup GPG
* Script: [preparation_before_release.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/preparation_before_release.sh)

* Usage
  ```
  ./beam/release/src/main/scripts/preparation_before_release.sh
  ```
* Tasks included
  1. Help you create a new GPG key if you want.
  1. Configure ```git user.signingkey``` with chosen pubkey.
  1. Add chosen pubkey into [dev KEYS](https://dist.apache.org/repos/dist/dev/beam/KEYS)  and [release KEYS](https://dist.apache.org/repos/dist/release/beam/KEYS)

     **NOTES**: Only PMC can write into [release repo](https://dist.apache.org/repos/dist/release/beam/).
  1. Start GPG agents.

__NOTE__: When generating the key, please make sure you choose the key type as __RSA and RSA (default)__ and key size as __4096 bit__.


##### Run all commands manually

* Get more entropy for creating a GPG key

      sudo apt-get install rng-tools
      sudo rngd -r /dev/urandom

* Create a GPG key

      gpg --full-generate-key

* Determine your Apache GPG Key and Key ID, as follows:

      gpg --list-sigs --keyid-format LONG

  This will list your GPG keys. One of these should reflect your Apache account, for example:

      --------------------------------------------------
      pub   2048R/845E6689 2016-02-23
      uid                  Nomen Nescio <anonymous@apache.org>
      sub   2048R/BA4D50BE 2016-02-23

  Here, the key ID is the 8-digit hex string in the `pub` line: `845E6689`.

  Now, add your Apache GPG key to the Beam’s `KEYS` file both in [`dev`](https://dist.apache.org/repos/dist/dev/beam/KEYS) and [`release`](https://dist.apache.org/repos/dist/release/beam/KEYS) repositories at `dist.apache.org`. Follow the instructions listed at the top of these files. (Note: Only PMC members have write access to the release repository. If you end up getting 403 errors ask on the mailing list for assistance.)

* Configure `git` to use this key when signing code by giving it your key ID, as follows:

      git config --global user.signingkey 845E6689

  You may drop the `--global` option if you’d prefer to use this key for the current repository only.

* Start GPG agent in order to unlock your GPG key

      eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
      export GPG_TTY=$(tty)
      export GPG_AGENT_INFO

#### Access to Apache Nexus repository

Configure access to the [Apache Nexus repository](https://repository.apache.org/), which enables final deployment of releases to the Maven Central Repository.

1. You log in with your Apache account.
1. Confirm you have appropriate access by finding `org.apache.beam` under `Staging Profiles`.
1. Navigate to your `Profile` (top right dropdown menu of the page).
1. Choose `User Token` from the dropdown, then click `Access User Token`. Copy a snippet of the Maven XML configuration block.
1. Insert this snippet twice into your global Maven `settings.xml` file, typically `${HOME}/.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` are your secret tokens:

        <!-- make sure you have the root `settings node: -->
        <settings>
          <servers>
            <server>
              <id>apache.releases.https</id>
              <username>TOKEN_NAME</username>
              <password>TOKEN_PASSWORD</password>
            </server>
            <server>
              <id>apache.snapshots.https</id>
              <username>TOKEN_NAME</username>
              <password>TOKEN_PASSWORD</password>
            </server>
          </servers>
        </settings>

#### Submit your GPG public key into MIT PGP Public Key Server
In order to make yourself have right permission to stage java artifacts in Apache Nexus staging repository,
please submit your GPG public key into [MIT PGP Public Key Server](http://pgp.mit.edu:11371/).

If MIT doesn't work for you (it probably won't, it's slow, returns 502 a lot, Nexus might error out not being able to find the keys),
use a keyserver at `ubuntu.com` instead: https://keyserver.ubuntu.com/.

#### Website development setup

Updating the Beam website requires submitting PRs to both the main `apache/beam`
repo and the `apache/beam-site` repo. The first contains reference manuals
generated from SDK code, while the second updates the current release version
number.

You should already have setup a local clone of `apache/beam`. Setting up a clone
of `apache/beam-site` is similar:

    $ git clone -b release-docs https://github.com/apache/beam-site.git
    $ cd beam-site
    $ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam-site.git
    $ git fetch --all
    $ git checkout -b <my-branch> origin/release-docs

Further instructions on website development on `apache/beam` is
[here](https://github.com/apache/beam/blob/master/website). Background
information about how the website is updated can be found in [Beam-Site
Automation Reliability](https://s.apache.org/beam-site-automation).

#### Register to PyPI

Release manager needs to have an account with PyPI. If you need one, [register at PyPI](https://pypi.python.org/account/register/). You also need to be a maintainer (or an owner) of the [apache-beam](https://pypi.python.org/pypi/apache-beam) package in order to push a new release. Ask on the mailing list for assistance.

#### Login to DockerHub
Run following command manually. It will ask you to input your DockerHub ID and password if
authorization info cannot be found from ~/.docker/config.json file.
```
docker login docker.io
```
After successful login, authorization info will be stored at ~/.docker/config.json file. For example,
```
"https://index.docker.io/v1/": {
   "auth": "xxxxxx"
}
```
Release managers should have push permission; please ask for help at dev@.
```
From: Release Manager
To: dev@beam.apache.org
Subject: DockerHub Push Permission

Hi DockerHub Admins

I need push permission to proceed with release, can you please add me to maintainer team?
My docker hub ID is: xxx

Thanks,
Release Manager
```
### Create a new version in JIRA

When contributors resolve an issue in JIRA, they are tagging it with a release that will contain their changes. With the release currently underway, new issues should be resolved against a subsequent future release. Therefore, you should create a release item for this subsequent release, as follows:

__Attention__: Only PMC has permission to perform this. If you are not a PMC, please ask for help in dev@ mailing list.

1. In JIRA, navigate to [`Beam > Administration > Versions`](https://issues.apache.org/jira/plugins/servlet/project-config/BEAM/versions).
1. Add a new release. Choose the next minor version number after the version currently underway, select the release cut date (today’s date) as the `Start Date`, and choose `Add`.
1. At the end of the release, go to the same page and mark the recently released version as released. Use the `...` menu and choose `Release`.


**********


## 3. Investigate performance regressions

Check the Beam load tests for possible performance regressions. Measurements are available on [metrics.beam.apache.org](http://metrics.beam.apache.org).

All Runners which publish data should be checked for the following, in both *batch* and *streaming* mode:

- [ParDo](http://metrics.beam.apache.org/d/MOi-kf3Zk/pardo-load-tests) and [GBK](http://metrics.beam.apache.org/d/UYZ-oJ3Zk/gbk-load-test): Runtime, latency, checkpoint duration
- [Nexmark](http://metrics.beam.apache.org/d/ahuaA_zGz/nexmark): Query runtime for all queries
- [IO](http://metrics.beam.apache.org/d/bnlHKP3Wz/java-io-it-tests-dataflow): Runtime

If regressions are found, the release branch can still be created, but the regressions should be investigated and fixed as part of the release process.
The role of the release manager is to file JIRA issues for each regression with the 'Fix Version' set to the to-be-released version. The release manager
oversees these just like any other JIRA issue marked with the 'Fix Version' of the release.

The mailing list should be informed to allow fixing the regressions in the course of the release.

## 4. Create a release branch in apache/beam repository

Attention: Only committer has permission to create release branch in apache/beam.

Release candidates are built from a release branch. As a final step in preparation for the release, you should create the release branch, push it to the Apache code repository, and update version information on the original branch.

There are 2 ways to cut a release branch: either running automation script(recommended), or running all commands manually.

#### Use cut_release_branch.sh to cut a release branch
* Script: [cut_release_branch.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/cut_release_branch.sh)

* Usage
  ```
  # Cut a release branch
  ./beam/release/src/main/scripts/cut_release_branch.sh \
  --release=${RELEASE_VERSION} \
  --next_release=${NEXT_VERSION}

  # Show help page
  ./beam/release/src/main/scripts/cut_release_branch.sh -h
  ```
* The script will:
  1. Create release-${RELEASE_VERSION} branch locally.
  1. Change and commit dev versoin number in master branch:

     [BeamModulePlugin.groovy](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy#L209),
     [gradle.properties](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/gradle.properties#L25),
     [version.py](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/sdks/python/apache_beam/version.py#L21)
  1. Change and commit version number in release branch:

     [version.py](https://github.com/apache/beam/blob/release-2.6.0/sdks/python/apache_beam/version.py#L21),
     [build.gradle](https://github.com/apache/beam/blob/release-2.6.0/runners/google-cloud-dataflow-java/build.gradle#L39),
     [gradle.properties](https://github.com/apache/beam/blob/release-2.16.0/gradle.properties#L27)

#### (Alternative) Run all steps manually
* Checkout working branch

  Check out the version of the codebase from which you start the release. For a new minor or major release, this may be `HEAD` of the `master` branch. To build a hotfix/incremental release, instead of the `master` branch, use the release tag of the release being patched. (Please make sure your cloned repository is up-to-date before starting.)

      git checkout <master branch OR release tag>

  **NOTE**: If you are doing an incremental/hotfix release (e.g. 2.5.1), please check out the previous release tag, rather than the master branch.

* Set up environment variables

  Set up a few environment variables to simplify Maven commands that follow. (We use `bash` Unix syntax in this guide.)

      RELEASE=2.5.0
      NEXT_VERSION_IN_BASE_BRANCH=2.6.0
      BRANCH=release-${RELEASE}

  Version represents the release currently underway, while next version specifies the anticipated next version to be released from that branch. Normally, 1.2.0 is followed by 1.3.0, while 1.2.3 is followed by 1.2.4.

  **NOTE**: Only if you are doing an incremental/hotfix release (e.g. 2.5.1), please check out the previous release tag, before running the following instructions:

      BASE_RELEASE=2.5.0
      RELEASE=2.5.1
      NEXT_VERSION_IN_BASE_BRANCH=2.6.0
      git checkout tags/${BASE_RELEASE}

* Create release branch locally

      git branch ${BRANCH}

* Update version files in the master branch.

      # Now change the version in existing gradle files, and Python files
      sed -i -e "s/'${RELEASE}'/'${NEXT_VERSION_IN_BASE_BRANCH}'/g" build_rules.gradle
      sed -i -e "s/${RELEASE}/${NEXT_VERSION_IN_BASE_BRANCH}/g" gradle.properties
      sed -i -e "s/${RELEASE}/${NEXT_VERSION_IN_BASE_BRANCH}/g" sdks/python/apache_beam/version.py

      # Save changes in master branch
      git add gradle.properties build_rules.gradle sdks/python/apache_beam/version.py
      git commit -m "Moving to ${NEXT_VERSION_IN_BASE_BRANCH}-SNAPSHOT on master branch."

* Check out the release branch.

      git checkout ${BRANCH}


* Update version files in release branch

      DEV=${RELEASE}.dev
      sed -i -e "s/${DEV}/${RELEASE}/g" sdks/python/apache_beam/version.py
      sed -i -e "s/${DEV}/${RELEASE}/g" gradle.properties
      sed -i -e "s/'beam-master-.*'/'beam-${RELEASE}'/g" runners/google-cloud-dataflow-java/build.gradle


### Start a snapshot build

Start a build of [the nightly snapshot](https://ci-beam.apache.org/job/beam_Release_NightlySnapshot/) against master branch.
Some processes, including our archetype tests, rely on having a live SNAPSHOT of the current version
from the `master` branch. Once the release branch is cut, these SNAPSHOT versions are no longer found,
so builds will be broken until a new snapshot is available.

There are 2 ways to trigger a nightly build, either using automation script(recommended), or perform all operations manually.

#### Run start_snapshot_build.sh to trigger build
* Script: [start_snapshot_build.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/start_snapshot_build.sh)

* Usage

      ./beam/release/src/main/scripts/start_snapshot_build.sh

* The script will:
  1. Install [hub](https://github.com/github/hub) with your agreement.
  1. Touch an empty txt file and commit changes into ```${your remote beam repo}/snapshot_build```
  1. Use hub to create a PR against apache:master, which triggers a Jenkins job to build snapshot.

* Tasks you need to do manually to __verify the SNAPSHOT build__
  1. Check whether the Jenkins job gets triggered. If not, please comment ```Run Gradle Publish``` into the generated PR.
  1. After verifying build succeeded, you need to close PR manually.

#### (Alternative) Do all operations manually

* Find one PR against apache:master in beam.
* Comment  ```Run Gradle Publish``` in this pull request to trigger build.
* Verify that build succeeds.


**********


## 5. Verify release branch

After the release branch is cut you need to make sure it builds and has no significant issues that would block the creation of the release candidate.
There are 2 ways to perform this verification, either running automation script(recommended), or running all commands manually.

! Dataflow tests will fail if Dataflow worker container is not created and published by this time. (Should be done by Google)

#### Run automation script (verify_release_build.sh)
* Script: [verify_release_build.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/verify_release_build.sh)

* Usage
  1. Create a personal access token from your Github account. See instruction [here](https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line).
     It'll be used by the script for accessing Github API.
     You don't have to add any permissions to this token.
  1. Update required configurations listed in `RELEASE_BUILD_CONFIGS` in [script.config](https://github.com/apache/beam/blob/master/release/src/main/scripts/script.config)
  1. Then run
     ```
     cd beam/release/src/main/scripts && ./verify_release_build.sh
     ```
  1. Trigger `beam_Release_Gradle_Build` and all PostCommit Jenkins jobs from PR (which is created by previous step).
     To do so, only add one trigger phrase per comment. See `JOB_TRIGGER_PHRASES` in [verify_release_build.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/verify_release_build.sh#L43)
     for full list of phrases.

* Tasks included in the script
  1. Installs ```hub``` with your agreement and setup local git repo;
  1. Create a test PR against release branch;

Jenkins job `beam_Release_Gradle_Build` basically run `./gradlew build -PisRelease`.
This only verifies that everything builds with unit tests passing.

You can use [mass_comment.py](https://github.com/apache/beam/blob/master/release/src/main/scripts/mass_comment.py) to mass-comment on PR.

#### Verify the build succeeds

* Tasks you need to do manually to __verify the build succeed__:
  1. Check the build result.
  2. If build failed, scan log will contain all failures.
  3. You should stabilize the release branch until release build succeeded.

There are some projects that don't produce the artifacts, e.g. `beam-test-tools`, you may be able to
ignore failures there.

To triage the failures and narrow things down you may want to look at `settings.gradle` and run the build only for the
projects you're interested at the moment, e.g. `./gradlew :runners:java-fn-execution`.

#### (Alternative) Run release build manually (locally)
* Pre-installation for python build
  1. Install pip

      ```
      curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
      python get-pip.py
      ```
  1. Install virtualenv

      ```
      pip install --upgrade virtualenv
      ```
  1. Cython

      ```
      sudo pip install cython
      sudo apt-get install gcc
      sudo apt-get install python-dev
      sudo apt-get install python3-dev
      sudo apt-get install python3.5-dev
      sudo apt-get install python3.6-dev
      sudo apt-get install python3.7-dev
      ```

* Run gradle release build

  1. Clean current workspace

      ```
      git clean -fdx
      ./gradlew clean
      ```

  1. Unlock the secret key
      ```
      gpg --output ~/doc.sig --sign ~/.bashrc
      ```

  1.  Run build command
      ```
      ./gradlew build -PisRelease --no-parallel --scan --stacktrace --continue
      ```

      To speed things up locally you might want to omit `--no-parallel`. You can also omit `--continue`
      if you want build fails after the first error instead of continuing, it may be easier and faster
      to find environment issues this way without having to wait until the full build completes.


#### Create release-blocking issues in JIRA

The verify_release_build.sh script may include failing or flaky tests. For each of the failing tests create a JIRA with the following properties:

* Issue Type: Bug

* Summary: Name of failing gradle task and name of failing test (where applicable) in form of :MyGradleProject:SomeGradleTask NameOfFailedTest: Short description of failure

* Priority: Major

* Component: "test-failures"

* Fix Version: Release number of verified release branch

* Description: Description of failure

#### Inform the mailing list

The dev@beam.apache.org mailing list should be informed about the release branch being cut. Alongside with this note,
a list of pending issues and to-be-trigated issues should be included. Afterwards, this list can be refined and updated
by the release manager and the Beam community.

**********


## 6. Triage release-blocking issues in JIRA

There could be outstanding release-blocking issues, which should be triaged before proceeding to build a release candidate. We track them by assigning a specific `Fix version` field even before the issue resolved.

The list of release-blocking issues is available at the [version status page](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel). Triage each unresolved issue with one of the following resolutions:

The release manager should triage what does and does not block a release. An issue should not block the release if the problem exists in the current released version or is a bug in new functionality that does not exist in the current released version. It should be a blocker if the bug is a regression between the currently released version and the release in progress and has no easy workaround.

For all JIRA issues:

* If the issue has been resolved and JIRA was not updated, resolve it accordingly.

For JIRA issues with type "Bug" or labeled "flaky":

* If the issue is a known continuously failing test, it is not acceptable to defer this until the next release. Please work with the Beam community to resolve the issue.
* If the issue is a known flaky test, make an attempt to delegate a fix. However, if the issue may take too long to fix (to the discretion of the release manager):
  * Delegate manual testing of the flaky issue to ensure no release blocking issues.
  * Update the `Fix Version` field to the version of the next release. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.

For all other JIRA issues:

* If the issue has not been resolved and it is acceptable to defer this until the next release, update the `Fix Version` field to the new version you just created. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the release cannot proceed. Instead, work with the Beam community to resolve the issue.

If there is a bug found in the RC creation process/tools, those issues should be considered high priority and fixed in 7 days.

### Review cherry-picks

Check if there are outstanding cherry-picks into the release branch, [e.g. for `2.14.0`](https://github.com/apache/beam/pulls?utf8=%E2%9C%93&q=is%3Apr+base%3Arelease-2.14.0).
Make sure they have blocker JIRAs attached and are OK to get into the release by checking with community if needed.

As the Release Manager you are empowered to accept or reject cherry-picks to the release branch. You are encouraged to ask the following questions to be answered on each cherry-pick PR and you can choose to reject cherry-pick requests if these questions are not satisfactorily answered:

* Is this a regression from a previous release? (If no, fix could go to a newer version.)
* Is this a new feature or related to a new feature? (If yes, fix could go to a new version.)
* Would this impact production workloads for users? (E.g. if this is a direct runner only fix it may not need to be a cherry pick.)
* What percentage of users would be impacted by this issue if it is not fixed? (E.g. If this is predicted to be a small number it may not need to be a cherry pick.)
* Would it be possible for the impacted users to skip this version? (If users could skip this version, fix could go to a newer version.)

It is important to accept major/blocking fixes to isolated issues to make a higher quality release. However, beyond that each cherry pick will increase the time required for the release and add more last minute code to the release branch. Neither late releases nor not fully tested code will provide positive user value.

_Tip_: Another tool in your toolbox is the known issues section of the release blog. Consider adding known issues there for minor issues instead of accepting cherry picks to the release branch.


**********


## 7. Build a release candidate

### Checklist before proceeding

* Release Manager’s GPG key is published to `dist.apache.org`;
* Release Manager’s GPG key is configured in `git` configuration;
* Release Manager has `org.apache.beam` listed under `Staging Profiles` in Nexus;
* Release Manager’s Nexus User Token is configured in `settings.xml`;
* JIRA release item for the subsequent release has been created;
* All test failures from branch verification have associated JIRA issues;
* There are no release blocking JIRA issues;
* Combined javadoc has the appropriate contents;
* Release branch has been created;
* There are no open pull requests to release branch;
* Originating branch has the version information updated to the new version;
* Nightly snapshot is in progress (do revisit it continually);

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

For this step, we recommend you using automation script to create a RC, but you still can perform all steps manually if you want.


### Run build_release_candidate.sh to create a release candidate

* Script: [build_release_candidate.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/build_release_candidate.sh)

* Usage

      ./beam/release/src/main/scripts/build_release_candidate.sh

* The script will:
  1. Run gradle release to create rc tag and push source release into github repo.
  1. Run gradle publish to push java artifacts into Maven staging repo.
  1. Stage source release into dist.apache.org dev [repo](https://dist.apache.org/repos/dist/dev/beam/).
  1. Stage, sign and hash python source distribution and wheels into dist.apache.org dev repo python dir
  1. Stage SDK docker images to [docker hub Apache organization](https://hub.docker.com/search?q=apache%2Fbeam&type=image).
  1. Create a PR to update beam-site, changes includes:
     * Copy python doc into beam-site
     * Copy java doc into beam-site

#### Tasks you need to do manually
  1. Verify the script worked.
      1. Verify that the source and Python binaries are present in [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam).
      1. Verify Docker images are published. How to find images:
          1. Visit [https://hub.docker.com/u/apache](https://hub.docker.com/search?q=apache%2Fbeam&type=image)
          2. Visit each repository and navigate to *tags* tab.
          3. Verify images are pushed with tags: ${RELEASE}_rc{RC_NUM}
      1. Verify that third party licenses are included in Docker containers by logging in to the images.
          - For Python SDK images, there should be around 80 ~ 100 dependencies.
          Please note that dependencies for the SDKs with different Python versions vary.
          Need to verify all Python images by replacing `${ver}` with each supported Python version `X.Y`.
          ```
          docker run -it --entrypoint=/bin/bash apache/beam_python${ver}_sdk:${RELEASE}_rc{RC_NUM}
          ls -al /opt/apache/beam/third_party_licenses/ | wc -l
          ```
          - For Java SDK images, there should be around 1400 dependencies.
          ```
          docker run -it --entrypoint=/bin/bash apache/beam_java${ver}_sdk:${RELEASE}_rc{RC_NUM}
          ls -al /opt/apache/beam/third_party_licenses/ | wc -l
          ```
  1. Publish staging artifacts
      1. Log in to the [Apache Nexus](https://repository.apache.org/#stagingRepositories) website.
      1. Navigate to Build Promotion -> Staging Repositories (in the left sidebar).
      1. Select repository `orgapachebeam-NNNN`.
      1. Click the Close button.
      1. When prompted for a description, enter “Apache Beam, version X, release candidate Y”.
      1. Review all staged artifacts on https://repository.apache.org/content/repositories/orgapachebeam-NNNN/. They should contain all relevant parts for each module, including `pom.xml`, jar, test jar, javadoc, etc. Artifact names should follow [the existing format](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22) in which artifact name mirrors directory structure, e.g., `beam-sdks-java-io-kafka`. Carefully review any new artifacts.

**********


## 8. Prepare documents

### Update and Verify Javadoc

The build with `-PisRelease` creates the combined Javadoc for the release in `sdks/java/javadoc`.

The file `sdks/java/javadoc/build.gradle` contains a list of modules to include
in and exclude, plus a list of offline URLs that populate links from Beam's
Javadoc to the Javadoc for other modules that Beam depends on.

* Confirm that new modules added since the last release have been added to the
  inclusion list as appropriate.

* Confirm that the excluded package list is up to date.

* Verify the version numbers for offline links match the versions used by Beam. If
  the version number has changed, download a new version of the corresponding
  `<module>-docs/package-list` file.


### Build the Pydoc API reference

Make sure you have ```tox``` installed:

```
pip install tox
```
Create the Python SDK documentation using sphinx by running a helper script.
```
cd sdks/python && pip install -r build-requirements.txt && tox -e py37-docs
```
By default the Pydoc is generated in `sdks/python/target/docs/_build`. Let `${PYDOC_ROOT}` be the absolute path to `_build`.

### Propose pull requests for website updates

Beam publishes API reference manuals for each release on the website. For Java
and Python SDKs, that’s Javadoc and PyDoc, respectively. The final step of
building the candidate is to propose website pull requests that update these
manuals.

Merge the pull requests only after finalizing the release. To avoid invalid
redirects for the 'current' version, merge these PRs in the order listed. Once
the PR is merged, the new contents will get picked up automatically and served
to the Beam website, usually within an hour.

**PR 1: apache/beam-site**

This pull request is against the `apache/beam-site` repo, on the `release-docs`
branch ([example](https://github.com/apache/beam-site/pull/603)).
It is created by `build_release_candidate.sh` (see above).

**PR 2: apache/beam**

This pull request is against the `apache/beam` repo, on the `master` branch ([example](https://github.com/apache/beam/pull/11727)).

* Update release version in `website/www/site/config.toml`.
* Add new release in `website/www/site/content/en/get-started/downloads.md`.
  * Download links will not work until the release is finalized.
* Update `website/www/site/static/.htaccess` to redirect to the new version.


### Blog post

Write a blog post similar to [beam-2.20.0.md](https://github.com/apache/beam/blob/master/website/www/site/content/en/blog/beam-2.20.0.md).

- Update `CHANGES.md` by adding a new section for the next release.
- Copy the changes for the current release from `CHANGES.md` to the blog post and edit as necessary.

__Tip__: Use git log to find contributors to the releases. (e.g: `git log --pretty='%aN' ^v2.10.0 v2.11.0 | sort | uniq`).
Make sure to clean it up, as there may be duplicate or incorrect user names.

__NOTE__: Make sure to include any breaking changes, even to `@Experimental` features,
all major features and bug fixes, and all known issues.

Template:

```
We are happy to present the new {$RELEASE_VERSION} release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/{$DOWNLOAD_ANCHOR}) for this release.
For more information on changes in {$RELEASE_VERSION}, check out the
[detailed release notes]({$JIRA_RELEASE_NOTES}).

## Highlights

  * New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
  * New highly anticipated feature Y added to JavaSDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

{$TOPICS e.g.:}
### I/Os
* Support for X source added (Java) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
{$TOPICS}

### New Features / Improvements

* X feature added (Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* Y feature added (Java) [BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y).

### Breaking Changes

* X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
* Y behavior was changed ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

### Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).

### Bugfixes

* Fixed X (Python) ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-X)).
* Fixed Y (Java) ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).

### Known Issues

* {$KNOWN_ISSUE_1}
* {$KNOWN_ISSUE_2}
* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%20{$RELEASE}%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.


## List of Contributors

According to git shortlog, the following people contributed to the 2.XX.0 release. Thank you to all contributors!

${CONTRIBUTORS}
```


#### Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of [repository.apache.org](https://repository.apache.org/content/repositories/)
1. Source distribution deployed to the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam/)
1. Website pull request proposed to list the [release](/get-started/downloads/), publish the [Java API reference manual](https://beam.apache.org/releases/javadoc/), and publish the [Python API reference manual](https://beam.apache.org/releases/pydoc/).
1. Docker images are published to [DockerHub](https://hub.docker.com/search?q=apache%2Fbeam&type=image) with tags: {RELEASE}_rc{RC_NUM}.

You can (optionally) also do additional verification by:
1. Check that Python zip file contains the `README.md`, `NOTICE`, and `LICENSE` files.
1. Check hashes (e.g. `md5sum -c *.md5` and `sha1sum -c *.sha1`)
1. Check signatures (e.g. `gpg --verify apache-beam-1.2.3-python.zip.asc apache-beam-1.2.3-python.zip`)
1. `grep` for legal headers in each file.
1. Run all jenkins suites and include links to passing tests in the voting email.
1. Pull docker images to make sure they are pullable.
```
docker pull {image_name}
docker pull apache/beam_python3.5_sdk:2.16.0_rc1
```


**********


## 9. Vote and validate release candidate

Once you have built and individually reviewed the release candidate, please share it for the community-wide review. Please review foundation-wide [voting guidelines](http://www.apache.org/foundation/voting.html) for more information.

Start the review-and-vote thread on the dev@ mailing list. Here’s an email template; please adjust as you see fit.

    From: Release Manager
    To: dev@beam.apache.org
    Subject: [VOTE] Release 1.2.3, release candidate #3

    Hi everyone,
    Please review and vote on the release candidate #3 for the version 1.2.3, as follows:
    [ ] +1, Approve the release
    [ ] -1, Do not approve the release (please provide specific comments)


    The complete staging area is available for your review, which includes:
    * JIRA release notes [1],
    * the official Apache source release to be deployed to dist.apache.org [2], which is signed with the key with fingerprint FFFFFFFF [3],
    * all artifacts to be deployed to the Maven Central Repository [4],
    * source code tag "v1.2.3-RC3" [5],
    * website pull request listing the release [6], publishing the API reference manual [7], and the blog post [8].
    * Java artifacts were built with Maven MAVEN_VERSION and OpenJDK/Oracle JDK JDK_VERSION.
    * Python artifacts are deployed along with the source release to the dist.apache.org [2].
    * Validation sheet with a tab for 1.2.3 release to help with validation [9].
    * Docker images published to Docker Hub [10].

    The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

    Thanks,
    Release Manager

    [1] https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=...
    [2] https://dist.apache.org/repos/dist/dev/beam/1.2.3/
    [3] https://dist.apache.org/repos/dist/release/beam/KEYS
    [4] https://repository.apache.org/content/repositories/orgapachebeam-NNNN/
    [5] https://github.com/apache/beam/tree/v1.2.3-RC3
    [6] https://github.com/apache/beam/pull/...
    [7] https://github.com/apache/beam-site/pull/...
    [8] https://github.com/apache/beam/pull/...
    [9] https://docs.google.com/spreadsheets/d/1qk-N5vjXvbcEk68GjbkSZTR8AGqyNUM-oLFo_ZXBpJw/edit#gid=...
    [10] https://hub.docker.com/search?q=apache%2Fbeam&type=image

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote. There’s no need to wait 72 hours. Proceed to the `Fix Issues` step below and address the problem. However, some issues don’t require cancellation. For example, if an issue is found in the website pull request, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email thread. Here’s an email template; please adjust as you see fit.

    From: Release Manager
    To: dev@beam.apache.org
    Subject: [RESULT] [VOTE] Release 1.2.3, release candidate #3

    I'm happy to announce that we have unanimously approved this release.

    There are XXX approving votes, XXX of which are binding:
    * approver 1
    * approver 2
    * approver 3
    * approver 4

    There are no disapproving votes.

    Thanks everyone!

### Run validation tests
All tests listed in this [spreadsheet](https://s.apache.org/beam-release-validation)

Since there are a bunch of tests, we recommend you running validations using automation script. In case of script failure, you can still run all of them manually.

#### Run validations using run_rc_validation.sh
* Script: [run_rc_validation.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/run_rc_validation.sh)

* Usage
  1. First update required configurations listed in `RC_VALIDATE_CONFIGS` in
     [script.config](https://github.com/apache/beam/blob/master/release/src/main/scripts/script.config)
  1. Then run
      ```
      ./beam/release/src/main/scripts/run_rc_validation.sh
      ```

* Tasks included
  1. Run Java quickstart with Direct Runner, Flink local runner, Spark local runner and Dataflow runner.
  1. Run Java Mobile Games(UserScore, HourlyTeamScore, Leaderboard) with Dataflow runner.
  1. Create a PR to trigger python validation job, including
     * Python quickstart in batch and streaming mode with direct runner and Dataflow runner.
     * Python Mobile Games(UserScore, HourlyTeamScore) with direct runner and Dataflow runner.
  1. Run Python Streaming MobileGames, includes
     * Start a new terminal to run Java Pubsub injector.
     * Start a new terminal to run python LeaderBoard with Direct Runner.
     * Start a new terminal to run python LeaderBoard with Dataflow Runner.
     * Start a new terminal to run python GameStats with Direct Runner.
     * Start a new terminal to run python GameStats with Dataflow Runner.

* Tasks you need to do manually
  1. Check whether validations succeed by following console output instructions.
  1. Terminate streaming jobs and java injector.
  1. Sign up [spreadsheet](https://s.apache.org/beam-release-validation).
  1. Vote in the release thread.

#### Run validations manually

_Note_: -Prepourl and -Pver can be found in the RC vote email sent by Release Manager.

* Java Quickstart Validation

  Direct Runner:
  ```
  ./gradlew :runners:direct-java:runQuickstartJavaDirect \
  -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-${KEY} \
  -Pver=${RELEASE_VERSION}
  ```
  Flink Local Runner
  ```
  ./gradlew :runners:flink:1.10:runQuickstartJavaFlinkLocal \
  -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-${KEY} \
  -Pver=${RELEASE_VERSION}
  ```
  Spark Local Runner
  ```
  ./gradlew :runners:spark:runQuickstartJavaSpark \
  -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-${KEY} \
  -Pver=${RELEASE_VERSION}
  ```
  Dataflow Runner
  ```
  ./gradlew :runners:google-cloud-dataflow-java:runQuickstartJavaDataflow \
  -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-${KEY} \
  -Pver=${RELEASE_VERSION} \
  -PgcpProject=${YOUR_GCP_PROJECT} \
  -PgcsBucket=${YOUR_GCP_BUCKET}
  ```
* Java Mobile Game(UserScore, HourlyTeamScore, Leaderboard)

  Pre-request
  * Create your own BigQuery dataset
    ```
    bq mk --project_id=${YOUR_GCP_PROJECT} ${YOUR_DATASET}
    ```
  * Create yout PubSub topic
    ```
    gcloud alpha pubsub topics create --project=${YOUR_GCP_PROJECT} ${YOUR_PROJECT_PUBSUB_TOPIC}
    ```
  * Setup your service account

    Goto IAM console in your project to create a service account as ```project owner```

    Run
    ```
    gcloud iam service-accounts keys create ${YOUR_KEY_JSON} --iam-account ${YOUR_SERVICE_ACCOUNT_NAME}@${YOUR_PROJECT_NAME}
    export GOOGLE_APPLICATION_CREDENTIALS=${PATH_TO_YOUR_KEY_JSON}
    ```
  Run
  ```
  ./gradlew :runners:google-cloud-dataflow-java:runMobileGamingJavaDataflow \
   -Prepourl=https://repository.apache.org/content/repositories/orgapachebeam-${KEY} \
   -Pver=${RELEASE_VERSION} \
   -PgcpProject=${YOUR_GCP_PROJECT} \
   -PgcsBucket=${YOUR_GCP_BUCKET} \
   -PbqDataset=${YOUR_DATASET} -PpubsubTopic=${YOUR_PROJECT_PUBSUB_TOPIC}
  ```
* Python Quickstart(batch & streaming), MobileGame(UserScore, HourlyTeamScore)

  Create a new PR in apache/beam

  In comment area, type in ```Run Python ReleaseCandidate```

* Python Leaderboard & GameStats
  * Get staging RC ```wget https://dist.apache.org/repos/dist/dev/beam/2.5.0/* ```
  * Verify the hashes

    ```
    sha512sum -c apache-beam-2.5.0-python.zip.sha512
    sha512sum -c apache-beam-2.5.0-source-release.zip.sha512
    ```
  * Build SDK

    ```
    sudo apt-get install unzip
    unzip apache-beam-2.5.0-source-release.zip
    python setup.py sdist
    ```
  * Setup virtualenv

    ```
    pip install --upgrade pip
    pip install --upgrade setuptools
    pip install --upgrade virtualenv
    virtualenv beam_env
     . beam_env/bin/activate
    ```
  * Install SDK

    ```
    pip install dist/apache-beam-2.5.0.tar.gz
    pip install dist/apache-beam-2.5.0.tar.gz[gcp]
    ```
  * Setup GCP

    Please repeat following steps for every following test.

    ```
    bq rm -rf --project=${YOUR_PROJECT} ${USER}_test
    bq mk --project_id=${YOUR_PROJECT} ${USER}_test
    gsutil rm -rf ${YOUR_GS_STORAGE]
    gsutil mb -p ${YOUR_PROJECT} ${YOUR_GS_STORAGE}
    gcloud alpha pubsub topics create --project=${YOUR_PROJECT} ${YOUR_PUBSUB_TOPIC}
    ```
    Setup your service account as described in ```Java Mobile Game``` section above.

    Produce data by using java injector:
    * Configure your ~/.m2/settings.xml as following:
      ```
      <settings>
        <profiles>
          <profile>
            <id>release-repo</id>
            <activation>
              <activeByDefault>true</activeByDefault>
            </activation>
            <repositories>
              <repository>
                <id>Release 2.4.0 RC3</id>
                <name>Release 2.4.0 RC3</name>
                <url>https://repository.apache.org/content/repositories/orgapachebeam-1031/</url>
              </repository>
            </repositories>
          </profile>
        </profiles>
      </settings>
      ```
      _Note_: You can found the latest  ```id```, ```name``` and ```url``` for one RC in the vote email thread sent out by Release Manager.

    * Run
      ```
      mvn archetype:generate \
            -DarchetypeGroupId=org.apache.beam \
            -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
            -DarchetypeVersion=${RELEASE_VERSION} \
            -DgroupId=org.example \
            -DartifactId=word-count-beam \
            -Dversion="0.1" \
            -Dpackage=org.apache.beam.examples \
            -DinteractiveMode=false
            -DarchetypeCatalog=internal

      mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector \
        -Dexec.args="${YOUR_PROJECT} ${YOUR_PUBSUB_TOPIC} none"
      ```
  * Run Leaderboard with Direct Runner
    ```
    python -m apache_beam.examples.complete.game.leader_board \
    --project=${YOUR_PROJECT} \
    --topic projects/${YOUR_PROJECT}/topics/${YOUR_PUBSUB_TOPIC} \
    --dataset ${USER}_test
    ```
    Inspect results:
    * Check whether there is any error messages in console.
    * Goto your BigQuery console and check whether your ${USER}_test has leader_board_users and leader_board_teams table.
    * bq head -n 10 ${USER}_test.leader_board_users
    * bq head -n 10 ${USER}_test.leader_board_teams
  * Run Leaderboard with Dataflow Runner
    ```
    python -m apache_beam.examples.complete.game.leader_board \
    --project=${YOUR_PROJECT} \
    --region=${GCE_REGION} \
    --topic projects/${YOUR_PROJECT}/topics/${YOUR_PUBSUB_TOPIC} \
    --dataset ${USER}_test \
    --runner DataflowRunner \
    --temp_location=${YOUR_GS_BUCKET}/temp/ \
    --sdk_location dist/*
    ```
    Inspect results:
    * Goto your Dataflow job console and check whether there is any error.
    * Goto your BigQuery console and check whether your ${USER}_test has leader_board_users and leader_board_teams table.
    * bq head -n 10 ${USER}_test.leader_board_users
    * bq head -n 10 ${USER}_test.leader_board_teams
  * Run GameStats with Direct Runner
    ```
    python -m apache_beam.examples.complete.game.game_stats \
    --project=${YOUR_PROJECT} \
    --topic projects/${YOUR_PROJECT}/topics/${YOUR_PUBSUB_TOPIC} \
    --dataset ${USER}_test \
    --fixed_window_duration ${SOME_SMALL_DURATION}
    ```
    Inspect results:
    * Check whether there is any error messages in console.
    * Goto your BigQuery console and check whether your ${USER}_test has game_stats_teams and game_stats_sessions table.
    * bq head -n 10 ${USER}_test.game_stats_teams
    * bq head -n 10 ${USER}_test.game_stats_sessions

  * Run GameStats with Dataflow Runner
    ```
    python -m apache_beam.examples.complete.game.game_stats \
    --project=${YOUR_PROJECT} \
    --region=${GCE_REGION} \
    --topic projects/${YOUR_PROJECT}/topics/${YOUR_PUBSUB_TOPIC} \
    --dataset ${USER}_test \
    --runner DataflowRunner \
    --temp_location=${YOUR_GS_BUCKET}/temp/ \
    --sdk_location dist/* \
    --fixed_window_duration ${SOME_SMALL_DURATION}
    ```
    Inspect results:
    * Goto your Dataflow job console and check whether there is any error.
    * Goto your BigQuery console and check whether your ${USER}_test has game_stats_teams and game_stats_sessions table.
    * bq head -n 10 ${USER}_test.game_stats_teams
    * bq head -n 10 ${USER}_test.game_stats_sessions


### Fix any issues

Any issues identified during the community review and vote should be fixed in this step. Additionally, any JIRA issues created from the initial branch verification should be fixed.

Code changes should be proposed as standard pull requests to the `master` branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

### Checklist to proceed to the next step

1. Issues identified during vote have been resolved, with fixes committed to the release branch.
2. All issues tagged with `Fix-Version` for the current release should be closed.
3. Community votes to release the proposed candidate, with at least three approving PMC votes


**********


## 10. Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be finalized. This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

### Deploy artifacts to Maven Central Repository

Use the [Apache Nexus repository manager](https://repository.apache.org/#stagingRepositories) to release the staged binary artifacts to the Maven Central repository. In the `Staging Repositories` section, find the relevant release candidate `orgapachebeam-XXX` entry and click `Release`. Drop all other release candidates that are not being released.
__NOTE__: If you are using [GitHub two-factor authentication](https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/) and haven't configure HTTPS access,
please follow [the guide](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) to configure command line access.

### Deploy Python artifacts to PyPI

* Script: [deploy_pypi.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/deploy_pypi.sh)
* Usage
```
./beam/release/src/main/scripts/deploy_pypi.sh
```
* Verify that the files at https://pypi.org/project/apache-beam/#files are correct.
All wheels should be published, in addition to the zip of the release source.
(Signatures and hashes do _not_ need to be uploaded.)

### Deploy SDK docker images to DockerHub
* Script: [publish_docker_images.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/publish_docker_images.sh)
* Usage
```
./beam/release/src/main/scripts/publish_docker_images.sh
```
Verify that:
* Images are published at [DockerHub](https://hub.docker.com/search?q=apache%2Fbeam&type=image) with tags {RELEASE} and *latest*.
* Images with *latest* tag are pointing to current release by confirming
  1. Digest of the image with *latest* tag is the same as the one with {RELEASE} tag.

### Merge Website pull requests

Merge all of the website pull requests
- [listing the release](/get-started/downloads/)
- publishing the [Python API reference manual](https://beam.apache.org/releases/pydoc/) and the [Java API reference manual](https://beam.apache.org/releases/javadoc/), and
- adding the release blog post.

### Git tag

Create and push a new signed tag for the released version by copying the tag for the final release candidate, as follows:

```
VERSION_TAG="v${RELEASE}"
git tag -s "$VERSION_TAG" "$RC_TAG"
git push https://github.com/apache/beam "$VERSION_TAG"
```

After the tag is uploaded, publish the release notes to Github, as follows:

```
cd beam/release/src/main/scripts && ./publish_github_release_notes.sh
```

Note this script reads the release notes from the blog post, so you should make sure to run this from master _after_ merging the blog post PR.


### PMC-Only Finalization
There are a few release finalization tasks that only PMC members have permissions to do. Ping [dev@](mailto:dev@beam.apache.org) for assistance if you need it.

#### Deploy source release to dist.apache.org

Copy the source release from the `dev` repository to the `release` repository at `dist.apache.org` using Subversion.

Make sure the last release's artifacts have been copied from `dist.apache.org` to `archive.apache.org`. This should happen automatically: [dev@ thread](https://lists.apache.org/thread.html/39c26c57c5125a7ca06c3c9315b4917b86cd0e4567b7174f4bc4d63b%40%3Cdev.beam.apache.org%3E) with context. The release manager should also make sure to change these links on the website ([example](https://github.com/apache/beam/pull/11727)).

#### Mark the version as released in JIRA

In JIRA, inside [version management](https://issues.apache.org/jira/plugins/servlet/project-config/BEAM/versions), hover over the current release and a settings menu will appear. Click `Release`, and select today’s date.

#### Recordkeeping with ASF

Use reporter.apache.org to seed the information about the release into future project reports.

### Checklist to proceed to the next step

* Maven artifacts released and indexed in the [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22)
* Source distribution available in the release repository of [dist.apache.org](https://dist.apache.org/repos/dist/release/beam/)
* Source distribution removed from the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam/)
* Website pull request to [list the release](/get-started/downloads/) and publish the [API reference manual](https://beam.apache.org/releases/javadoc/) merged
* Release tagged in the source code repository
* Release version finalized in JIRA. (Note: Not all committers have administrator access to JIRA. If you end up getting permissions errors ask on the mailing list for assistance.)
* Release version is listed at reporter.apache.org


**********


## 11. Promote the release

Once the release has been finalized, the last step of the process is to promote the release within the project and beyond.

### Apache mailing lists

Announce on the dev@ mailing list that the release has been finished.

Announce on the release on the user@ mailing list, listing major improvements and contributions.

Announce the release on the announce@apache.org mailing list.
__NOTE__: This can only be done from `@apache.org` email address.


### Social media

Tweet, post on Facebook, LinkedIn, and other platforms. Ask other contributors to do the same.

Also, update [the Wikipedia article on Apache Beam](https://en.wikipedia.org/wiki/Apache_Beam).

### Checklist to declare the process completed

1. Release announced on the user@ mailing list.
1. Blog post published, if applicable.
1. Release recorded in reporter.apache.org.
1. Release announced on social media.
1. Completion declared on the dev@ mailing list.
1. Update Wikipedia Apache Beam article.

**********

## Improve the process

It is important that we improve the release processes over time. Once you’ve finished the release, please take a step back and look what areas of this process and be improved. Perhaps some part of the process can be simplified. Perhaps parts of this guide can be clarified.

If we have specific ideas, please start a discussion on the dev@ mailing list and/or propose a pull request to update this guide. Thanks!
