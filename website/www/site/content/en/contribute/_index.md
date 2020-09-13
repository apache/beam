---
title: "Beam Contribution Guide"
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

# Apache Beam Contribution Guide

The Apache Beam community welcomes contributions from anyone!

If you have questions, please [reach out to the Beam community](/contribute/get-help).

There are lots of opportunities to contribute:

 - ask or answer questions on [user@beam.apache.org](/community/contact-us/) or
[stackoverflow](https://stackoverflow.com/questions/tagged/apache-beam)
 - review proposed design ideas on [dev@beam.apache.org](/community/contact-us/)
 - improve the documentation
 - file [bug reports](https://issues.apache.org/jira/projects/BEAM/issues)
 - test releases
 - review [changes](https://github.com/apache/beam/pulls)
 - write new examples
 - improve your favorite language SDK (Java, Python, Go, etc)
 - improve specific runners (Apache Flink, Apache Spark, Google
   Cloud Dataflow, etc)
 - improve or add IO connectors
 - add new transform libraries (statistics, ML, image processing, etc)
 - work on the core programming model (what is a Beam pipeline and how does it
   run?)
 - improve the developer experience (for example, Windows guides)
 - add answers to the [contribution FAQ](
 https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ)
 - organize local meetups of users or contributors to Apache Beam

Most importantly, if you have an idea of how to contribute, then do it!

## Contributing code

Below is a tutorial for contributing code to Beam, covering our tools and typical process in
detail.

### Prerequisites

To contribute code, you need

 - a GitHub account
 - a Linux, macOS, or Microsoft Windows development environment with Java JDK 8 installed
 - [Docker](https://www.docker.com/) installed for some tasks including building worker containers and testing this website
   changes locally
 - [Go](https://golang.org) 1.12 or later installed for Go SDK development
 - Python 2.7, 3.5, 3.6, and 3.7. Yes, you need all four versions installed.
    - pip, setuptools, virtualenv, and tox installed for Python development
 - for large contributions, a signed [Individual Contributor License
   Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
   Software Foundation (ASF).

To install these in a Debian-based distribution:

```
sudo apt-get install \
   openjdk-8-jdk \
   python-setuptools \
   python-pip \
   virtualenv \
   tox \
   docker-ce
```

You also need to [install Go](https://golang.org/doc/install).

Once Go is installed, install goavro:

```
$ export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
$ go get github.com/linkedin/goavro
```

gLinux users should configure their machines for sudoless Docker.

Alternatively, you can use the [Dockerfile](https://github.com/apache/beam/blob/master/Dockerfile) to setup a container meeting the requirements above, and mounting the directory that contains the Beam repo to the container. e.g.:
```shell script
docker run -it --mount type=bind,source=/Users/jsmith/Desktop/beam,target=/workspaces/beam <CONTAINER_ID> bash
```

### Connect With the Beam community

1. Consider subscribing to the [dev@ mailing list](/community/contact-us/), especially
   if you plan to make more than one change or the change will be large. All decisions happen on the
   public dev list.
1. (Optionally) Join the [#beam channel of the ASF slack](/community/contact-us/).
1. Create an account on [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues)
   (anyone can do this).

### Share your intent

1. Find or create an issue in the [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues).
   Tracking your work in an issue will avoid duplicated or conflicting work, and provide
   a place for notes. Later, your pull request will be linked to the issue as well.
1. If you want to get involved but don't have a project in mind, check our list of open starter tasks,
   [https://s.apache.org/beam-starter-tasks](https://s.apache.org/beam-starter-tasks).
1. Assign the issue to yourself. To get the permission to do so, email
   the [dev@ mailing list](/community/contact-us)
   to introduce yourself and to be added as a contributor in the Beam issue tracker including your
   ASF Jira Username. For example [this welcome email](
   https://lists.apache.org/thread.html/e6018c2aaf7dc7895091434295e5b0fafe192b975e3e3761fcf0cda7@%3Cdev.beam.apache.org%3E).
1. If your change is large or it is your first change, it is a good idea to
   [discuss it on the dev@ mailing list](/community/contact-us/).
1. For large changes create a design doc
   ([template](https://s.apache.org/beam-design-doc-template),
   [examples](https://s.apache.org/beam-design-docs)) and email it to the [dev@ mailing list](/community/contact-us).

### Development Setup

1. If you need help with git forking, cloning, branching, committing, pull requests, and squashing commits, see
   [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)
1. Familiarize yourself with gradle and the project structure. At the root of the git repository, run:

       $ ./gradlew projects

   Examine the available tasks in a project. For the default set of tasks, use:

       $ ./gradlew tasks

   For a given module, use:

       $ ./gradlew -p sdks/java/io/cassandra tasks

    For an exhaustive list of tasks, use:

       $ ./gradlew tasks --all

1. Make sure you can build and run tests

    Run the entire set of tests with:

       $ ./gradlew check

   You can limit testing to a particular module. Gradle will build just the necessary things to run those tests. For example:

       $ ./gradlew -p sdks/go check
       $ ./gradlew -p sdks/java/io/cassandra check
       $ ./gradlew -p runners/flink check

1. Now you may want to set up your preferred IDE and other aspects of your development
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

1. Make your code change. Every source file needs to include the Apache license header. Every new dependency needs to
   have an open source license [compatible](https://www.apache.org/legal/resolved.html#criteria) with Apache.

1. Add unit tests for your change.

1. Use descriptive commit messages that make it easy to identify changes and provide a clear history.

1. When your change is ready to be reviewed and merged, create a pull request.

1. Format commit messages and the pull request title like `[BEAM-XXX] Fixes bug in ApproximateQuantiles`,
   where you replace BEAM-XXX with the appropriate JIRA issue.
   This will automatically link the pull request to the issue.

1. The pull request and any changes pushed to it will trigger [pre-commit
   jobs](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide#ContributionTestingGuide-Pre-commit). If a test fails and appears unrelated to your
   change, you can cause tests to be re-run by adding a single line comment on your
   PR

        retest this please

   Pull request template has a link to a [catalog of trigger phrases](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md)
   that start various post-commit tests suites. Use these sparingly because post-commit tests consume shared development resources.

1. Pull requests can only be merged by a
   [Beam committer](https://home.apache.org/phonebook.html?pmc=beam).
   To find a committer for your area, either:
    - look in the OWNERS file of the directory where you changed files, or
    - look for similar code merges, or
    - ask on [dev@beam.apache.org](/community/contact-us/)

   Use `R: @username` in the pull request to notify a reviewer.

1. If you don't get any response in 3 business days, email the [dev@ mailing list](/community/contact-us) to ask for someone to look at your pull
   request.

### Make the reviewer's job easier

1. Provide context for your changes in the associated JIRA issue and/or PR description.

1. Avoid huge mega-changes.

1. Review feedback typically leads to follow-up changes. It is easier to review follow-up changes when they are added as additional "fixup" commits to the
   existing PR/branch. This allows reviewer(s) to track the incremental progress and focus on new changes,
   and keeps comment threads attached to the code.
   Please refrain from squashing new commits into reviewed commits before review is completed.
   Because squashing reviewed and unreviewed commits often makes it harder to
   see the the difference between the review iterations, reviewers may ask you to unsquash new changes.

1. After review is complete and the PR is accepted, fixup commits should be squashed (see [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)).
   Beam committers [can squash](https://beam.apache.org/contribute/committer-guide/#merging-it)
   all commits in the PR during merge, however if a PR has a mixture of independent changes that should not be squashed, and fixup commits,
   then the PR author should help squashing fixup commits to maintain a clean commmit history.

## When will my change show up in an Apache Beam release?

Apache Beam makes minor releases every 6 weeks. Apache Beam has a
[calendar](https://calendar.google.com/calendar/embed?src=0p73sl034k80oob7seouanigd0%40group.calendar.google.com) for
cutting the next release branch. Your change needs to be checked into master before the release branch is cut
to make the next release.

## Stale pull requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future. The associated JIRAs will be
unassigned from the author but will stay open.

## Accounts and Permissions

- [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues):
  Anyone can access it and browse issues. Anyone can register an account and login
  to create issues or add comments. Only contributors can be assigned issues. If
  you want to be assigned issues, a PMC member can add you to the project contributor
  group.  Email the [dev@ mailing list](/community/contact-us)
  to ask to be added as a contributor in the Beam issue tracker, and include your ASF Jira username.

- [Beam Wiki Space](https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam):
  Anyone has read access. If you wish to contribute changes, please create an account and request edit access on the
  [dev@ mailing list](/community/contact-us) (include your Wiki account user ID).

- Pull requests can only be merged by a
  [Beam committer](https://home.apache.org/phonebook.html?pmc=beam).

- [Voting on a release](https://www.apache.org/foundation/voting.html): Everyone can vote. Only
  [Beam PMC](https://home.apache.org/phonebook.html?pmc=beam) members should mark their votes as binding.

## Communication

All communication is expected to align with the [Code of Conduct](https://www.apache.org/foundation/policies/conduct).

Discussion about contributing code to Beam happens on the [dev@ mailing list](/community/contact-us/). Introduce yourself!

Questions can be asked on the [#beam channel of the ASF slack](/community/contact-us/). Introduce yourself!

## Additional resources

If you are contributing a `PTransform` to Beam, we have an extensive
[PTransform Style Guide](/contribute/ptransform-style-guide).

If you are contributing a Runner to Beam, refer to the
[Runner authoring guide](/contribute/runner-guide/)

Review [design documents](https://s.apache.org/beam-design-docs).

A great way to contribute is to join an existing effort. For the most
intensive efforts, check out the [roadmap](/roadmap/).

You can also find a more exhaustive list on the [Beam developers' wiki](
https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam)

## Troubleshooting

If you run into any issues, check out the [contribution FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ) or ask on the [dev@ mailing list](/community/contact-us/) or [#beam channel of the ASF slack](/community/contact-us/).

If you didn't find the information you were looking for in this guide, please
[reach out to the Beam community](/community/contact-us).
