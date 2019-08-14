---
layout: section
title: "Beam Release Guide: 02 - Create Branch"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/create-branch/
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

# Create the release branch

*****


Release candidates are built from a release branch. As a final step in preparation for the release,
you should create the release branch in `apache/beam/ repository, push it to the Apache code repository,
and update version information on the original branch.



## Use `cut_release_branch.sh` to cut the release branch

* Script: [cut_release_branch.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/cut_release_branch.sh)

* Usage
  ```sh
  # Cut a release branch
  ./beam/release/src/main/scripts/cut_release_branch.sh \
  --release=${RELEASE_VERSION} \
  --next_release=${NEXT_VERSION}
  
  # Show help page
  ./beam/release/src/main/scripts/cut_release_branch.sh -h
  ```

* The script will:
  1. Create `release-${RELEASE_VERSION}` branch locally.
  1. Change and commit dev versoin number in master branch:
  
     [BeamModulePlugin.groovy](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy#L209),
     [gradle.properties](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/gradle.properties#L25),
     [version.py](https://github.com/apache/beam/blob/e8abafe360e126818fe80ae0f6075e71f0fc227d/sdks/python/apache_beam/version.py#L21)
  1. Change and commit version number in release branch:
  
     [version.py](https://github.com/apache/beam/blob/release-2.6.0/sdks/python/apache_beam/version.py#L21), 
     [build.gradle](https://github.com/apache/beam/blob/release-2.6.0/runners/google-cloud-dataflow-java/build.gradle#L39)


*****

## (Alternative) Create the release branch manually

Instead of running `cut_release_branch.sh` you can run all the commands manually. 

__NOTE__: For each step make sure you understand what the script does before running the commands.

Run the commands from the script one-by-one manually:

* Use Bash, as it's what the scripts are written for;

* Check out the version of the codebase from which you start the release. For a new minor or major release, this may be `HEAD` of the `master` branch. To build a hotfix/incremental release, instead of the `master` branch, use the release tag of the release being patched.
  (Please make sure your cloned repository is up-to-date before starting.)

  **NOTE**: If you are doing an incremental/hotfix release (e.g. 2.5.1), please check out the previous release tag, rather than the master branch.

* Set up a few environment variables to simplify Maven commands that follow, e.g. `RELEASE`, `NEXT_VERSION_IN_BASE_BRANCH`, etc.;
  
  **NOTE**: Only if you are doing an incremental/hotfix release (e.g. 2.5.1), please check out the previous release tag;

  **NOTE**: After creating the release branch the script updates the master branch with the next version;

* Create release branch locally;

* Update version files in the `master` branch. E.g. `build_rules.gradle`, `gradle.properties`, `version.py`;

* Update version files in release branch, e.g. `version.py`, `build.gradle` in Dataflow runner project;e


*****


## Start a snapshot build

Start a build of [the nightly snapshot](https://builds.apache.org/view/A-D/view/Beam/job/beam_Release_NightlySnapshot/) against master branch.
Some processes, including our archetype tests, rely on having a live `SNAPSHOT` of the current version
from the `master` branch. Once the release branch is cut, these `SNAPSHOT` versions are no longer found,
so builds will be broken until a new snapshot is available.


### Run `start_snapshot_build.sh` to trigger build

* Script: [start_snapshot_build.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/start_snapshot_build.sh)

* Usage
  ```sh
  ./beam/release/src/main/scripts/start_snapshot_build.sh
  ```

* The script does the following:
  1. Installs [hub](https://github.com/github/hub) with your agreement;
  1. Touches an empty `txt` file and commits changes into ```${your remote beam repo}/snapshot_build```;
  1. Uses `hub` to create a PR against `master`, which triggers a Jenkins job to build a snapshot;


### Verify the `SNAPSHOT` build  

Tasks you need to do manually
  1. Check whether the Jenkins job gets triggered. If not, please comment ```Run Gradle Publish``` into the generated PR.
  1. After verifying build succeeded, you need to close PR manually.

*****

### (Alternative) Start the snapshot build manually

* Find one PR against `master` in Beam;
* Comment  ```Run Gradle Publish``` in this pull request to trigger build;
* Verify that build succeeds;

*****

<a class="button button--primary" href="{{'/contribute/release-guide/verify-branch/'|prepend:site.baseurl}}">Next Step: Verify Branch</a>
