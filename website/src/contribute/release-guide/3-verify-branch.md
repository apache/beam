---
layout: section
title: "Beam Release Guide: 03 - Verify Branch"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/verify-branch/
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

# Verify the release branch

*****

After the release branch is cut you need to make sure it builds and has no significant issues that would block the
creation of the release candidate.


## Run `verify_release_build.sh`
* Script: [verify_release_build.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/verify_release_build.sh)

* Usage

  ```sh
  ./beam/release/src/main/scripts/verify_release_build.sh
  ```

* The script does the following:
  1. Installs ```pip```, ```virtualenv```, ```cython``` and ```/usr/bin/time``` with your agreements;
  2. Runs ```gradle release build``` against release branch;

Basically it does `./gradlew build -PisRelease` from the root of Beam repository.
This only verifies that everything builds with unit tests passing.

To speed things up locally you might want to omit `--no-parallel`.
You might want to omit `--continue` if you want the script to fail after the first error instead of continuing,
it may be easier and faster to find environment issues this way without having to wait until the full build completes.

There are some projects that don't produce the artifacts, e.g. `beam-test-tools``, you may be able to
ignore failures there.

To triage the failures and narrow things down you may want to look at `settings.gradle` and run the build only for the
projects you're interested at the moment, e.g. `./gradlew :runners:java-fn-execution`. 



### Verify the build succeeds

Tasks you need to do manually:
  1. Check the build result;
  2. If build failed, scan log will contain all failures;
  3. You should stabilize the release branch until release build succeeded;
  4. The script will output a set of Jenkins phrases to enter in the created PR;

*****

## (Alternative) Run all commands manually

Follow the scripts and run all the steps manually, overall you need to do:

* Install python dependencies, e.g. `pip`, `virtualenv`, `cython`;

* Run gradle release build, along these lines (check the script):

  1. Unlock the secret key, e.g.:

     ```sh
     gpg --output ~/doc.sig --sign ~/.bashrc
     ```

  1. Run build command, e.g.:

     ```sh
     ./gradlew build -PisRelease
     ```


*****


## Create release-blocking issues in JIRA

The `verify_release_build.sh` script may include failing or flaky tests. For each of the failing tests create a JIRA with the following properties:

* Issue Type: Bug

* Summary: Name of failing gradle task and name of failing test (where applicable) in form of *:MyGradleProject:SomeGradleTask NameOfFailedTest: Short description of failure*

* Priority: Major

* Component: "test-failures"

* Fix Version: Release number of verified release branch

* Description: Description of failure


*****

## Checklist to proceed to the next step

* Release Manager’s GPG key is published to `dist.apache.org`;
* Release Manager’s GPG key is configured in `git` configuration;
* Release Manager has `org.apache.beam` listed under `Staging Profiles` in Nexus;
* Release Manager’s Nexus User Token is configured in `settings.xml`;
* JIRA release item for the subsequent release has been created;
* All test failures from branch verification have associated JIRA issues;
* There are no release blocking JIRA issues;
* Release Notes in JIRA have been audited and adjusted;
* Combined javadoc has the appropriate contents;
* Release branch has been created;
* There are no open pull requests to release branch;
* Originating branch has the version information updated to the new version;
* Nightly snapshot is in progress (do revisit it continually);



**********

<a class="button button--primary" href="{{'/contribute/release-guide/build-candidate/'|prepend:site.baseurl}}">Next Step: Build the Release Candidate</a>
