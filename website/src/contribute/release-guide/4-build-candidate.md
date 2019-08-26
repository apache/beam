---
layout: section
title: "Beam Release Guide: 04 - Build Candidate"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/build-candidate/
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

# Build a release candidate


*****


## Checklist before proceeding

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


## Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate.
The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

For this step, we recommend you using automation script to create a RC, but you still can perform all steps manually if you want. 

### Run `build_release_candidate.sh` to create a release candidate

* Script: [build_release_candidate.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/build_release_candidate.sh)

* Usage
  
  ```
  ./beam/release/src/main/scripts/build_release_candidate.sh
  ```

* The script does the following:
  1. Runs gradle release to create rc tag and push source release into github repo.
  1. Runs gradle publish to push java artifacts into Maven staging repo.
     
     __NOTE__: In order to public staging artifacts, you need to goto the staging repo to close the staging repository on Apache Nexus. 
     When prompted for a description, enter “Apache Beam, version X, release candidate Y”.
  1. Stages source release into dist.apache.org dev [repo](https://dist.apache.org/repos/dist/dev/beam/);
  1. Stages, signs and hashes python binaries into dist.apache.ord dev repo python dir;
  1. Creates a PR to update beam and beam-site, changes includes:
     * Copy python doc into beam-site;
     * Copy java doc into beam-site;
     * Updates release version in [_config.yml](https://github.com/apache/beam/blob/master/website/_config.yml);
     
### Tasks you need to do manually:
  1. Add new release into `website/src/get-started/downloads.md`;
  1. Update last release download links in `website/src/get-started/downloads.md`;
  1. Update `website/src/.htaccess` to redirect to the new version;


*****

## (Alternative) Run all steps manually

#### Build and stage Java artifacts with Gradle

For all steps check what the script does:

* Set up a few environment variables to simplify the commands that follow. These identify the release candidate being built,
  and the branch where you will stage files. Start with `RC_NUM` equal to `1` and increment it for each candidate;

* Make sure your git config will maintain your account;

* Use Gradle release plugin to build the release artifacts, and push code and
  release tag to the origin repository (this would be the Apache Beam repo);

* Use Gradle publish plugin to stage these artifacts on the Apache Nexus repository;

* Review all staged artifacts. They should contain all relevant parts for each module,
  including `pom.xml`, `jar`, `-test.jar`, `javadoc`, etc. Artifact names should follow 
  [the existing format](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22).
  
  Carefully review any new artifacts.
  
#### Stage source release on dist.apache.org

__Attention__: Only PMC members have permissions to perform following steps.

* Copy the source release to the dev repository of `dist.apache.org`:

  1. Check out the Beam section of the `dev` repository on `dist.apache.org` via Subversion;
  1. Make a directory for the new release;
  1. Download source zip from GitHub;
  1. Create hashes and sign the source distribution;
  1. Add and commit all the files;
  1. Verify that files are [present](https://dist.apache.org/repos/dist/dev/beam).

#### Stage python binaries on dist.apache.org

  1. Build python binaries in release branch in sdks/python dir.
  1. Create hashes and sign the binaries

### Close the staging repository on Apache Nexus

Go to Nexus UI find the staging repository, hit the close button. 
When prompted for a description, enter “Apache Beam, version X, release candidate Y”.

If there were mistakes found, you can drop the staging repository and re-run the publication step.



*****

## Build and stage python wheels

There is a wrapper repo [beam-wheels](https://github.com/apache/beam-wheels) to help build python wheels.

If you are interested in how it works, please refer to the [structure section](https://github.com/apache/beam-wheels#structure).

Please follow the [user guide](https://github.com/apache/beam-wheels#user-guide) to build python wheels.

Once all python wheels have been staged [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam/), 
please run [./sign_hash_python_wheels.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/sign_hash_python_wheels.sh) to sign and hash python wheels.


*****
<a class="button button--primary" href="{{'/contribute/release-guide/triage-jira/'|prepend:site.baseurl}}">Next Step: Triage Blockers</a>
