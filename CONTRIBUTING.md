<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Contributing to Beam

There are many ways to contribute to Beam, just one of which is by contributing code.
For a full list of ways to contribute and get plugged into Beam, see the
[Beam Contribution Guide](https://beam.apache.org/contribute/get-started-contributing/)

## Code Contributions

*Before opening a pull request*, review the Beam contribution guide below.
It lists steps that are required before creating a PR and provides tips for
getting started. In particular, consider the following:

- Have you searched for existing, related Issues and pull requests?
- Have you shared your intent by creating an issue and commenting that you plan to take it on?
- If the change is large, have you discussed it on the dev@ mailing list?
- Is the change being proposed clearly explained and motivated?

These steps and instructions on getting started are outlined below as well.

### Prerequisites

- A [GitHub](https://github.com/) account.
- A Linux, macOS, or Microsoft Windows development environment.
- Java JDK 8 installed.
- [Go](https://golang.org) 1.16.0 or later installed.
- [Docker](https://www.docker.com/) installed for some tasks including building worker containers and testing changes to this website locally.
- For SDK Development:
  - Python 3.x interpreters. You will need Python interpreters for all Python versions supported by Beam.
    Interpreters should be installed and available in shell via `python3.x` commands. For more information, see:
    Python installation tips in [Developer Wiki](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips#PythonTips-InstallingPythoninterpreters).
- For large contributions, a signed [Individual Contributor License.
  Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
  Software Foundation (ASF).

### Share Your Intent
1. Find or create an issue in the [Beam repo](https://github.com/apache/beam/issues/new/choose).
   Tracking your work in an issue will avoid duplicated or conflicting work, and provide
   a place for notes. Later, your pull request will be linked to the issue as well.
2. Comment ".take-issue" on the issue. This will cause the issue to be assigned to you.
   When you've completed the issue, you can close it by commenting ".close-issue".
   If you are a committer and would like to assign an issue to a non-committer, they must comment
   on the issue first; please tag the user asking them to do so or to comment "\`.take-issue\`".
   The command will be ignored if it is surrounded by `\`` markdown characters.
3. If your change is large or it is your first change, it is a good idea to
   [discuss it on the dev@beam.apache.org mailing list](https://beam.apache.org/community/contact-us/).
4. For large changes create a design doc
   ([template](https://s.apache.org/beam-design-doc-template),
   [examples](https://s.apache.org/beam-design-docs)) and email it to the [dev@beam.apache.org mailing list](https://beam.apache.org/community/contact-us/).

### Setup Your Environment and Learn About Language Specific Setup

Before you begin, check out the Wiki pages. There are many useful tips about [Git](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips), [Go](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips), [Gradle](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips), [Java](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips), [Python](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips), etc.

#### Configuration Options
You have two options for configuring your development environment:
- Local:
  - Manually installing the [prerequisites](https://beam.apache.org/contribute/#prerequisites).
  - Using the automated script for Linux and macOS.
- Container-based: using a [Docker](https://www.docker.com/) image.

##### Local: Debian-based Distribution

###### Manual steps

To install these in a Debian-based distribution:
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
2. On some systems, like Ubuntu 20.04, install these:
    ```
    pip3 install grpcio-tools mypy-protobuf
    ```
3. If you develop in GO:
  1. Install [Go](https://golang.org/doc/install).
  2. Check BEAM repo is in: `$GOPATH/src/github.com/apache/`
  3. At the end, it should look like this: `$GOPATH/src/github.com/apache/beam`
4. Once Go is installed, install goavro:
    ```
    $ export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
    $ go get github.com/linkedin/goavro/v2
    ```
**Important**: gLinux users should configure their machines for sudoless Docker.

###### Automated script for Linux and macOS

You can install these in a Debian-based distribution for Linux or macOs using the [local-env-setup.sh](https://github.com/apache/beam/blob/master/local-env-setup.sh) script, which is part of the Beam repo. It contains:

* pip3 packages
* go packages
* goavro
* JDK 8
* Python
* Docker

To install execute:
```
./local-env-setup.sh
```

##### Container: Docker-based

Alternatively, you can use the Docker based local development environment to wrap your clone of the Beam repo
into a container meeting the requirements above.

You can start this container using the [start-build-env.sh](https://github.com/apache/beam/blob/master/start-build-env.sh) script which is part of the Beam repo.

Execute:
```
./start-build-env.sh
```

#### Development Setup {#development-setup}

1. Check [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips) if you need help with git forking, cloning, branching, committing, pull requests, and squashing commits.

2. Make a fork of https://github.com/apache/beam repo.

3. Clone the forked repository. You can download it anywhere you like.
    ```
    $ mkdir -p ~/path/to/your/folder
    $ cd ~/path/to/your/folder
    $ git clone https://github.com/forked/apache/beam
    $ cd beam
    ```
   For **Go development**:

   We recommend putting it in your `$GOPATH` (`$HOME/go` by default on Unix systems).

   Clone the repo, and update your branch as normal:
    ```
    $ git clone https://github.com/apache/beam.git
    $ cd beam
    $ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam.git
    $ git fetch --all
    ```

   Get or Update all the Go SDK dependencies:
    ```
    $ go get -u ./...
    ```

4. Check the environment was set up correctly.

   **Option 1**: validate the Go, Java, and Python environments:

   **Important**: Make sure you have activated Python development.
    ```
    ./gradlew :checkSetup
    ```
   **Option 2**: Run independent checks:
  - For **Go development**:
      ```
      export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore./gradlew :sdks:go:examples:wordCount
      ```
  - For **Python development**:
      ```
      ./gradlew :sdks:python:wordCount
      ```
  - For **Java development**:
      ```
      ./gradlew :examples:java:wordCount
     ```

5. Familiarize yourself with gradle and the project structure.

   At the root of the git repository, run:
    ```
    $ ./gradlew projects
    ```
   Examine the available tasks in a project. For the default set of tasks, use:
    ```
    $ ./gradlew tasks
    ```
   For a given module, use:
    ```
    $ ./gradlew -p sdks/java/io/cassandra tasks
    ```
   For an exhaustive list of tasks, use:
    ```
    $ ./gradlew tasks --all
    ```

6. Make sure you can build and run tests.

   Since Beam is a large project, usually, you will want to limit testing to the particular module you are working on. Gradle will build just the necessary things to run those tests. For example:
    ```
    $ ./gradlew -p sdks/go check
    $ ./gradlew -p sdks/java/io/cassandra check
    $ ./gradlew -p runners/flink check
    ```

7. Now you may want to set up your preferred IDE and other aspects of your development
   environment. See the Developers' wiki for tips, guides, and FAQs on:
  - [IntelliJ](https://cwiki.apache.org/confluence/display/BEAM/Using+IntelliJ+IDE)
  - [Java](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips)
  - [Python](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)
  - [Go](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips)
  - [Website](https://cwiki.apache.org/confluence/display/BEAM/Website+Tips)
  - [Gradle](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips)
  - [Jenkins](https://cwiki.apache.org/confluence/display/BEAM/Jenkins+Tips)
  - [FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ)

### Create a Pull Request

1. Make your code change. Every source file needs to include the Apache license header. Every new dependency needs to
   have an open source license [compatible](https://www.apache.org/legal/resolved.html#criteria) with Apache.

2. Add unit tests for your change.

3. Use descriptive commit messages that make it easy to identify changes and provide a clear history.

4. When your change is ready to be reviewed and merged, create a pull request.

5. Link to the issue you are addressing in your pull request.

6. The pull request and any changes pushed to it will trigger [pre-commit
   jobs](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide#ContributionTestingGuide-Pre-commit). If a test fails and appears unrelated to your
   change, you can cause tests to be re-run by adding a single line comment on your
   PR:
    ```
    retest this please
    ```
Pull request template has a link to a [catalog of trigger phrases](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md)
that start various post-commit tests suites. Use these sparingly because post-commit tests consume shared development resources.

### Review Process and Releases

#### Get Reviewed

Your pull requests should automatically have reviewers assigned within a few hours of opening it.
If that doesn't happen for some reason, you can also request a review yourself.

1. Pull requests can only be merged by a
   [Beam committer](https://home.apache.org/phonebook.html?pmc=beam).
   To find a committer for your area, either:
  - look for similar code merges, or
  - ask on [dev@beam.apache.org](https://beam.apache.org/community/contact-us/)

   Use `R: @username` in the pull request to notify a reviewer.

2. If you don't get any response in 3 business days, email the [dev@beam.apache.org mailing list](https://beam.apache.org/community/contact-us/) to ask for someone to look at your pull request.

#### Make the Reviewer’s Job Easier

1. Provide context for your changes in the associated issue and/or PR description.

2. Avoid huge mega-changes.

3. Review feedback typically leads to follow-up changes. It is easier to review follow-up changes when they are added as additional "fixup" commits to the
   existing PR/branch. This allows reviewer(s) to track the incremental progress and focus on new changes,
   and keeps comment threads attached to the code.
   Please refrain from squashing new commits into reviewed commits before review is completed.
   Because squashing reviewed and unreviewed commits often makes it harder to
   see the difference between the review iterations, reviewers may ask you to unsquash new changes.

4. After review is complete and the PR is accepted, fixup commits should be squashed (see [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)).
   Beam committers [can squash](https://beam.apache.org/contribute/committer-guide/#merging-it)
   all commits in the PR during merge, however if a PR has a mixture of independent changes that should not be squashed, and fixup commits,
   then the PR author should help squashing fixup commits to maintain a clean commit history.

#### Apache Beam Releases

Apache Beam makes minor releases every 6 weeks. Apache Beam has a
[calendar](https://calendar.google.com/calendar/embed?src=0p73sl034k80oob7seouanigd0%40group.calendar.google.com) for
cutting the next release branch. Your change needs to be checked into master before the release branch is cut
to make the next release.

#### Stale Pull Requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future.

### Troubleshooting

If you run into any issues, check out the [contribution FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ) or ask on the [dev@ mailing list](https://beam.apache.org/community/contact-us/) or [#beam channel of the ASF Slack](https://beam.apache.org/community/contact-us/).

If you didn't find the information you were looking for in this guide, please
[reach out to the Beam community](https://beam.apache.org/community/contact-us/).

</div>

## Find Efforts to Contribute to
A great way to contribute is to join an existing effort. If you want to get involved but don’t have a project in mind, check our [list of open starter tasks](https://s.apache.org/beam-starter-tasks).
For the most intensive efforts, check out the [roadmap](https://beam.apache.org/roadmap/).

## Contributing to the Developer Documentation

New contributors are often best equipped to find gaps in the developer documentation.
If you'd like to contribute to our documentation, either open a PR in the Beam repo with
the proposed changes or make edits to the [Beam wiki](https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam).

By default, everyone has access to the wiki. If you wish to contribute changes,
please create an account and request edit access on the dev@beam.apache.org mailing list (include your Wiki account user ID).

## Additional Resources
Please see Beam developers’ [Wiki Contributor FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ) for more information.

If you are contributing a ```PTransform``` to Beam, we have an extensive [PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide).

If you are contributing a Runner to Beam, refer to the [Runner authoring guide](https://beam.apache.org/contribute/runner-guide/).

Review [design documents](https://s.apache.org/beam-design-docs).