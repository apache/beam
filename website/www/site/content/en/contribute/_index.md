---
title: "Beam Contribution Guide"
type: "contribute"
layout: "arrow_template"
aliases:
  - /contribution-guide/
  - /contribute/contribution-guide/
  - /docs/contribute/
  - /contribute/source-repository/
  - /contribute/design-principles/
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

# Contribution guide

<a class="arrow-list-header" data-toggle="collapse" href="#collapseOverview" role="button" aria-expanded="false"        aria-controls="collapseOverview">
   {{< figure src="/images/arrow-icon_list.svg">}}

## Overview

</a>

<div class="collapse dont-collapse-sm" id="collapseOverview">

### Audience
This guide is for new contributors for Apache Beam.

### How to Contribute
There are lots of opportunities to contribute. You can, for example:

- Ask or answer questions on [user@beam.apache.org](/community/contact-us/) or
[stackoverflow](https://stackoverflow.com/questions/tagged/apache-beam).
 - Review proposed design ideas on [dev@beam.apache.org](/community/contact-us/).
 - Improve the documentation.
 - File [bug reports](https://issues.apache.org/jira/projects/BEAM/issues).
 - Test releases and review [changes](https://github.com/apache/beam/pulls).
 - Write new examples.
 - Improve your favorite language SDK, for example, Java, Python, or Go.
 - Improve specific runners, for example, Apache Flink, Apache Spark, or Google
   Cloud Dataflow.
 - Improve or add IO connectors.
 - Add new transform libraries for example, statistics, ML,or  image processing.
 - Work on the core programming model: what is a Beam pipeline and how does it
   run?
 - Improve the developer experience, for example, Windows guides.
 - Add answers to the [contribution FAQ](
 https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ).
 - Organize local meetups of users or contributors to Apache Beam.

</div>

<a class="arrow-list-header" data-toggle="collapse" href="#collapseContributing" role="button" aria-expanded="false" aria-controls="collapseContributing">
   {{< figure src="/images/arrow-icon_list.svg">}}

## Contributing code

  </a>

<div class="collapse dont-collapse-sm" id="collapseContributing">

Below is a tutorial for contributing code to Beam, covering our tools and typical process in
detail.

### Prerequisites

 - A GitHub account.
 - A Linux, macOS, or Microsoft Windows development environment.
    - Required *only* For Windows:
      - Java JDK 8 installed.
      - [Docker](https://www.docker.com/) installed for some tasks including building worker containers and testing this website
   changes locally.
 - For Go SDK development:
    - [Go](https://golang.org) 1.12 or later installed.
 - For Python development: Python 3.6, 3.7, and 3.8. Yes, you need all three versions installed.
    - Required *only* For Windows:
      - pip, setuptools, virtualenv, and tox installed
 - For large contributions, a signed [Individual Contributor License
   Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
   Software Foundation (ASF).

### Configuration options

You have two options for configuring your development environment:

- Local: Debian-based Distribution
  - Manual steps
  - Automated script (*only* for Linux and macOS)
- Container: Docker-based

**Important**: gLinux users should configure their machines for sudoless Docker.

#### Local: Debian-based Distribution

##### Manual steps

To install these in a Debian-based distribution using manual steps:
1. Execute:
    ```
       sudo apt-get install \
       openjdk-8-jdk \
       python-setuptools \
       python-pip \
       virtualenv \
       tox \
       docker-ce
    ```
1. On some systems (like Ubuntu 20.04) these need to be installed also
    ```
    pip3 install grpcio-tools mypy-protobuf
    ```
1. If you you develop in GO:
    1. [Install Go](https://golang.org/doc/install).
    1. BEAM repo should be in: `$GOPATH/src/github.com/apache/`
    At the end, it should look like this:
    `$GOPATH/src/github.com/apache/beam`
1. Install goavro:
    ```
    $ export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
    $ go get github.com/linkedin/goavro
    ```

##### Automated script for Linux and macOS

You can install these in a Debian-based distribution for Linux or macOs using the [local-env-setup.sh](https://github.com/apache/beam/blob/master/local-env-setup.sh)
script, which is part of the Beam repo:

1. Execute:
    ```
    ./local-env-setup.sh
    ```

#### Container: Docker-based


Alternatively, you can use the Docker-based local development environment to wrap your clone of the Beam repo
into a container meeting the requirements above.

You can start this container using the [start-build-env.sh](https://github.com/apache/beam/blob/master/start-build-env.sh)
script, which is part of the Beam repo:

1. Execute:
    ```
    ./start-build-env.sh
    ```

### Connect With the Beam community

1. Subscribe to the [dev@ mailing list](/community/contact-us/), all decisions happen on the
   public dev list.
1. (Optionally) Join the [#beam channel of the ASF slack](/community/contact-us/).
1. Create an account on [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues)
   (anyone can do this).

### Share your intent

1. Find or create an issue in the [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues).
   Tracking your work in an issue will avoid duplicated or conflicting work, provides
   a place for notes and your pull request is linked to the issue.
1. If you don't have a project in mind, check our list of
   [open starter tasks](https://s.apache.org/beam-starter-tasks).
1. Assign the issue to yourself. To get the permission to do so:  
  1. Email
   the [dev@ mailing list](/community/contact-us)
   to introduce yourself and to be added as a contributor in the Beam issue tracker, include your
   ASF Jira Username. For example [this welcome email](https://lists.apache.org/thread.html/e6018c2aaf7dc7895091434295e5b0fafe192b975e3e3761fcf0cda7@%3Cdev.beam.apache.org%3E).
1. If your change is large or it is your first change,
   [discuss it on the dev@ mailing list](/community/contact-us/).
1. For large changes:
  1. Create a design doc using this
   [template](https://s.apache.org/beam-design-doc-template).
  1. Check these
   [examples](https://s.apache.org/beam-design-docs).
  1. Email it to the [dev@ mailing list](/community/contact-us).

### Development Setup {#development-setup}

1. Check [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips) if you need help with git forking, cloning, branching, committing, pull requests, and squashing commits.

1. Clone the git repository. You can download it anywhere you like.
  **Important**: For Go development, we recommend putting it in your [`$GOPATH`](https://golang.org/doc/gopath_code#GOPATH) (`$HOME/go` by default on Unix systems).

       $ mkdir -p ~/go/src/github.com/apache
       $ cd ~/go/src/github.com/apache
       $ git clone https://github.com/apache/beam
       $ cd beam

  1. For Go development, prepare your environment to check it was set sucessfully.
      1. Clone the repo, and update your branch as normal
      
              $ git clone https://github.com/apache/beam.git
              $ cd beam
              $ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam.git
              $ git fetch --all
              
      1. Get or Update all the Go SDK dependencies
              
              $ go get -u ./...

1. Check the environment was setup correctly.
    - **Option 1**: Run independent checks:
        - For **Go development**:
          1. Execute:
              ```
              export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore
              ./gradlew :sdks:go:examples:wordCount
              ```
        - For **Python development**:
          1. Activate your environment:
              ```
              python3 -m venv ~/.virtualenvs/env
              . ~/.virtualenvs/bin/activate
              ```
          1. Execute:
              `./gradlew :sdks:python:wordCount`

        - For **Java development**:
          1. Execute:
              `./gradlew :examples:java:wordCount`
   - **Option 2**: validate the Go, Java, and Python environments:
          ```
          ./gradlew :checkSetUp
          ```
      
        **Important**: Make sure you have activated Python development.

1. Familiarize yourself with gradle and the project structure.
    1. At the root of the git repository, run:

            $ ./gradlew projects

    1. Examine the available tasks in a project. For the default set of tasks, use:

            $ ./gradlew tasks

    1. For a given module, use:

            $ ./gradlew -p sdks/java/io/cassandra tasks

    1. For an exhaustive list of tasks, use:

            $ ./gradlew tasks --all

1. Make sure you can build and run tests

   Since Beam is a large project, usually, you will want to limit testing to the particular module you are working on. Gradle will build just the necessary things to run those tests. For example:

              $ ./gradlew -p sdks/go check
              $ ./gradlew -p sdks/java/io/cassandra check
              $ ./gradlew -p runners/flink check

1. Set up your preferred IDE and other aspects of your development
   environment. See the Developers' wiki for tips, guides, and FAQs on:
   - [IntelliJ](https://cwiki.apache.org/confluence/display/BEAM/Using+IntelliJ+IDE)
   - [Java](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips)
   - [Python](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)
   - [Go](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips)
   - [Website](https://cwiki.apache.org/confluence/display/BEAM/Website+Tips)
   - [Gradle](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips)
   - [Jenkins](https://cwiki.apache.org/confluence/display/BEAM/Jenkins+Tips)
   - [FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ)

### Make your change

1. Make your code change.
      - Every source file needs to include the Apache license header.
      - Every new dependency needs to have an open-source license [compatible](https://www.apache.org/legal/resolved.html#criteria) with Apache.
1. Add unit tests for your change.
1. Use descriptive commit messages that make it easy to identify changes and provide a clear history.
1. When your change is ready to be reviewed and merged, create a pull request.
1. Format commit messages and the pull request title like this:
        `[BEAM-XXX] Fixes bug in ApproximateQuantiles`
    **Important**: Replace BEAM-XXX with the appropriate JIRA issue.
    This will automatically link the pull request to the issue.
1. The pull request and any changes pushed to it will trigger [pre-commit
   jobs](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide#ContributionTestingGuide-Pre-commit). If a test fails and appears unrelated to your
   change, you can cause tests to be re-run by adding a single line comment on your
   PR:

        retest this please

   Pull request template has a link to a [catalog of trigger phrases](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md)
   that start various post-commit tests suites. Use these sparingly because post-commit tests consume shared development resources.
1. Pull requests can only be merged by a
   [Beam committer](https://home.apache.org/phonebook.html?pmc=beam).
   To find a committer for your area, either:
      - Look in the OWNERS file of the directory where you changed files.
      - Look for similar code merges.
      - Ask on [dev@beam.apache.org](/community/contact-us/).

   **Note**: Use `R: @username` in the pull request to notify a reviewer.
1. If you don't get any response in 3 business days, email the [dev@ mailing list](/community/contact-us) to ask for someone to look at your pull
   request.

### Make the reviewer's job easier

1. **Provide context for your changes** in the associated JIRA issue and/or PR description.
1. **Avoid huge mega-changes**.
1. Review feedback typically leads to follow-up changes. Consider the following to make it easier to review follow-up changes:
   - **Add follow-up changes as additional "fixup" commits** to the
   existing PR/branch. This allows reviewer(s) to track the incremental progress, focus on new changes,
   and keeps comment threads attached to the code.
   - **Refrain from squashing new commits** into reviewed commits before review is completed. Reviewers may ask you to "unsquash" new changes because squashing reviewed and unreviewed commits make it harder to see the difference between the review iterations.
1. After review is complete and the PR is accepted, **fixup commits should be squashed** (see [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)).
   Beam committers [can squash](https://beam.apache.org/contribute/committer-guide/#merging-it)
   all commits in the PR during merge, however if a PR has a mixture of independent changes that should not be squashed, and fixup commits,
   then the PR author should help squashing fixup commits to maintain a clean commit history.


### When will my change show up in an Apache Beam release?

Apache Beam makes minor releases every 6 weeks. Apache Beam has a
[calendar](https://calendar.google.com/calendar/embed?src=0p73sl034k80oob7seouanigd0%40group.calendar.google.com) for
cutting the next release branch. Your change needs to be checked into master before the release branch is cut
to make the next release.

</div>

## Stale pull requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future. The associated JIRAs will be
unassigned from the author but will stay open.

## Accounts and Permissions

- [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues):
  - Everyone can access it and browse issues.
  - Everyone can register an account and login
  to create issues or add comments.
  - Only contributors can be assigned issues. If
  you want to be assigned issues, a PMC member can add you to the project contributor
  group:
    1. Email the [dev@ mailing list](/community/contact-us)
  to ask to be added as a contributor in the Beam issue tracker, and include your ASF Jira username.

- [Beam Wiki Space](https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam):
  - Everyone has read access.
  If you wish to contribute changes:
    1. Create an account
    1. Request edit access on the
  [dev@ mailing list](/community/contact-us) (include your Wiki account user ID).

- Pull requests:
  - Can only be merged by a
  [Beam committer](https://home.apache.org/phonebook.html?pmc=beam).

- [Voting on a release](https://www.apache.org/foundation/voting.html):
  - Everyone can vote.
  - Only
  [Beam PMC](https://home.apache.org/phonebook.html?pmc=beam) members should mark their votes as binding.

## Communication

Follow these recommendations to keep a clear and effective communication:

- Align your communication with the [Code of Conduct](https://www.apache.org/foundation/policies/conduct).

- Introduce yourself and discuss about contributing code to Beam on the [dev@ mailing list](/community/contact-us/).

- Ask for questions on the [#beam channel of the ASF slack](/community/contact-us/).

## Additional resources

Check the following resources to have a significant contribution:

- If you are contributing a `PTransform` to Beam, check the
[PTransform Style Guide](/contribute/ptransform-style-guide).

- If you are contributing a `Runner` to Beam, refer to the
[Runner authoring guide](/contribute/runner-guide/).

- Review [design documents](https://s.apache.org/beam-design-docs).

- Join an existing effort, it is a great way to contribute. For the most
intensive efforts, check out the [roadmap](/roadmap/).

Find a more exhaustive list on the [Beam developers' wiki](
https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam)

## Troubleshooting

If you run into any issues:
  - Check out the [contribution FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ).
  - Ask on the [dev@ mailing list](/community/contact-us/).
  - Ask on the [#beam channel of the ASF slack](/community/contact-us/).

If you didn't find the information you were looking for in this guide, reach out to the [Beam community](/community/contact-us).
