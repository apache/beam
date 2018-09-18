---
layout: section
title: "Beam Contribution Guide"
permalink: /contribute/
section_menu: section-menu/contribute.html
redirect_from:
 - /contribution-guide/
 - /contribute/contribution-guide/
 - /docs/contribute/
 - /contribute/feature-branches/
 - /contribute/work-in-progress/
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

There are lots of opportunities:

 - ask or answer questions on [user@beam.apache.org]({{ site.baseurl
}}/community/contact-us/) or
[stackoverflow](https://stackoverflow.com/questions/tagged/apache-beam)
 - review proposed design ideas on [dev@beam.apache.org]({{ site.baseurl
}}/community/contact-us/)
 - improve the documentation
 - contribute [bug reports](https://issues.apache.org/jira/projects/BEAM/issues)
 - write new examples
 - add new user-facing libraries (new statistical libraries, new IO connectors,
   etc)
 - improve your favorite language SDK (Java, Python, Go, etc)
 - improve specific runners (Apache Apex, Apache Flink, Apache Spark, Google
   Cloud Dataflow, etc)
 - work on the core programming model (what is a Beam pipeline and how does it
   run?)
 - improve the developer experience on Windows

Most importantly, if you have an idea of how to contribute, then do it!

For a list of open starter tasks, check
[https://s.apache.org/beam-starter-tasks](https://s.apache.org/beam-starter-tasks).

## Permissions

For the [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues), 
anyone can access it and browse issues. Anyone can register an account and login 
to create issues or add comments. Only contributors can be assigned issues. If 
you want to be assigned issues, a PMC member can add you to the project contributor
group.  Email the [dev@ mailing list]({{ site.baseurl }}/community/contact-us)
to ask to be added as a contributor in the Beam issue tracker.

## Contributing code

Discussons about contributing code to beam  happens on the [dev@ mailing list]({{ site.baseurl
}}/community/contact-us/). Introduce yourself!

Questions can be asked on the [#beam channel of the ASF slack]({{ site.baseurl
}}/community/contact-us/). Introduce yourself!

Coding happens at
[https://github.com/apache/beam](https://github.com/apache/beam). To
contribute, follow the usual GitHub process: fork the repo, make your changes,
and open a pull request and @mention a reviewer. If you have more than one commit
in your change, you many be asked to rebase and squash the commits.
If you are unfamiliar with this workflow, GitHub maintains these helpful guides:

 - [Git Handbook](https://guides.github.com/introduction/git-handbook/)
 - [Forking a repository](https://guides.github.com/activities/forking/)

If your change is large or it is your first change, it is a good idea to
[discuss it on the dev@ mailing list]({{ site.baseurl }}/community/contact-us/)

For large changes (you may be asked to create a design doc
([template](https://s.apache.org/beam-design-doc-template),
[examples](https://s.apache.org/beam-design-docs))).

Documentation happens at [https://github.com/apache/beam-site](https://github.com/apache/beam-site)
and contributions are welcome.

Large contributions require a signed [Individual Contributor License
Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
Software Foundation (ASF).

If you are contributing a `PTransform` to Beam, we have an extensive
[PTransform Style Guide]({{ site.baseurl }}/contribute/ptransform-style-guide).

### Building & Testing

We use the [Gradle Build Tool](https://gradle.org/).

You do not need to install Gradle, but you do need a Java SDK installed. You can develop on Linux, macOS, or Microsoft Windows. There have been issues noted when developing using Windows; feel free to contribute fixes to make it easier.

Familiarize yourself with the project structure. At the root of the git repository, run:

    $ ./gradlew projects

Run the entire set of tests with:

    $ ./gradlew check

You can limit testing to a particular module. Gradle will build just the necessary things to run those tests. For example:

    $ ./gradlew -p sdks/go check
    $ ./gradlew -p sdks/java/io/cassandra check
    $ ./gradlew -p runners/flink check

Examine the available tasks in a project. For the default set of tasks, use:

    $ ./gradlew tasks

For a given module, use:

    $ ./gradlew sdks/java/io/cassandra tasks

For an exhaustive list of tasks, use:

    $ ./gradlew tasks --all

We run **integration and performance test** using [Jenkins](https://jenkins.io/). The job definitions are available in the [Beam GitHub repository](https://github.com/apache/beam/tree/master/.test-infra/jenkins).

#### Troubleshooting

You might get an OutOfMemoryException during the Gradle build. If you have more memory
available, you can try to increase the memory allocation of the Gradle JVM. Otherwise,
disabling parallel test execution reduces memory consumption. In the root of the Beam
source, edit the `gradle.properties` file and add/modify the following lines:

    org.gradle.parallel=false
    org.gradle.jvmargs=-Xmx2g -XX:MaxPermSize=512m

### Pull requests

When your change is ready to be reviewed and merged, create a pull request.
Format the pull request title like `[BEAM-XXX] Fixes bug in ApproximateQuantiles`,
where you replace BEAM-XXX with the appropriate JIRA issue.
This will automatically link the pull request to the issue.

Pull requests can only be merged by a
[beam committer](https://people.apache.org/phonebook.html?unix=beam).
To find a committer for your area, look for similar code merges or ask on
[dev@beam.apache.org]({{ site.baseurl }}/community/contact-us/)

Use @mention in the pull request to notify the reviewer.

The pull request and any changes pushed to it will trigger [pre-commit
jobs](/contribute/testing/). If a test fails and appears unrelated to your
change, you can cause tests to be re-run by adding a single line comment on your
PR

     retest this please

There are other trigger phrases for post-commit tests found in
.testinfra/jenkins, but use these sparingly because post-commit
tests consume shared development resources.

### Developing with the Python SDK

Gradle can build and test python, and is used by the Jenkins jobs, so needs to
be maintained.

You can directly use the Python toolchain instead of having Gradle orchestrate
it, which may be faster for you, but it is your preference.
If you do want to use Python tools directly, we recommend setting up a virtual
environment before testing your code.

If you update any of the [cythonized](http://cython.org) files in Python SDK,
you must install the `cython` package before running following command to
properly test your code.

The following commands should be run in the `sdks/python` directory.
This installs Python from source and includes the test and gcp dependencies.

On macOS/Linux:

    $ virtualenv env
    $ . ./env/bin/activate
    (env) $ pip install -e .[gcp,test]

On Windows:

    > c:\Python27\python.exe -m virtualenv
    > env\Scripts\activate
    (env) > pip install -e .[gcp,test]

This command runs all Python tests. The nose dependency is installed by [test] in pip install.

    (env) $ python setup.py nosetests

You can use following command to run a single test method.

    (env) $ python setup.py nosetests --tests <module>:<test class>.<test method>

    For example:
    (env) $ python setup.py nosetests --tests apache_beam.io.textio_test:TextSourceTest.test_progress

You can deactivate the virtualenv when done.

    (env) $ deactivate
    $

To check just for Python lint errors, run the following command.

    $ ../../gradlew lint

Or use `tox` commands to run the lint tasks:

    $ tox -e py27-lint    # For python 2.7
    $ tox -e py3-lint     # For python 3
    $ tox -e py27-lint3   # For python 2-3 compatibility

#### Remote testing

This step is only required for testing SDK code changes remotely (not using
directrunner). In order to do this you must build the Beam tarball. From the
root of the git repository, run:

```
$ cd sdks/python/
$ python setup.py sdist
```

Pass the `--sdk_location` flag to use the newly built version. For example:

```
$ python setup.py sdist > /dev/null && \
    python -m apache_beam.examples.wordcount ... \
        --sdk_location dist/apache-beam-2.5.0.dev0.tar.gz
```

## Reviews

Reviewers for [apache/beam](https://github.com/apache/beam) are listed in
Prow-style OWNERS files. A description of these files can be found
[here](https://go.k8s.io/owners).

### Finding reviewers

Currently this is a manual process. Tracking bug for automating this:
[BEAM-4790](https://issues.apache.org/jira/browse/BEAM-4790).

For each file to be reviewed, look for an OWNERS file in its directory. Pick a
single reviewer from that file. If the directory doesn't contain an OWNERS file,
go up a directory. Keep going until you find one. Try to limit the number of
reviewers to 2 per PR if possible, to minimize reviewer load.

### Adding yourself as a reviewer

Find the deepest sub-directory that contains the files you want to be a reviewer
for and add your Github username under `reviewers` in the OWNERS file (create a
new OWNERS file if necessary).

The Beam project currently only uses the `reviewers` key in OWNERS and no other
features, as reviewer selection is still a manual process.

<!-- TODO(BEAM-4790): If Prow write access gets approved
(https://issues.apache.org/jira/browse/INFRA-16869), document that if you are
not a committer you can still be listed as a reviewer. Just ask to get added as
a read-only collaborator to apache/beam by opening an INFRA ticket. -->

## Contributing to the website

The Beam website is in the [Beam Site GitHub
mirror](https://github.com/apache/beam-site) repository in the `asf-site`
branch (_not_ `master`).  The
[README](https://github.com/apache/beam-site/blob/asf-site/README.md) there
explains how to modify different parts of the site. The GitHub workflow is the
same - make your change and open a pull request.

Issues are tracked in the
[website](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20website)
component in JIRA.

## Works in progress

A great way to contribute is to join an existing effort. There are many
works in progress, some on branches because they are very incomplete.

### Portability Framework

The primary Beam vision: Any SDK on any runner. This is a cross-cutting effort
across Java, Python, and Go, and every Beam runner.

 - [Read more]({{ site.baseurl }}/contribute/portability/)

### Apache Spark 2.0 Runner

 - Feature branch: [runners-spark2](https://github.com/apache/beam/tree/runners-spark2)
 - Contact: [Jean-Baptiste Onofré](mailto:jbonofre@apache.org)

### JStorm Runner

 - [Docs]({{ site.baseurl }}/documentation/runners/jstorm)
 - Feature branch: [jstorm-runner](https://github.com/apache/beam/tree/jstorm-runner)
 - JIRA: [runner-jstorm](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-jstorm) / [BEAM-1899](https://issues.apache.org/jira/browse/BEAM-1899)
 - Contact: [Pei He](mailto:pei@apache.org)

### MapReduce Runner

 - Feature branch: [mr-runner](https://github.com/apache/beam/tree/mr-runner)
 - JIRA: [runner-mapreduce](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-mapreduce) / [BEAM-165](https://issues.apache.org/jira/browse/BEAM-165)
 - Contact: [Pei He](mailto:pei@apache.org)

### Tez Runner

 - Feature branch: [tez-runner](https://github.com/apache/beam/tree/tez-runner)
 - JIRA: [runner-tez](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-tez) / [BEAM-2709](https://issues.apache.org/jira/browse/BEAM-2709)

### Go SDK

 - JIRA: [sdk-go](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go) / [BEAM-2083](https://issues.apache.org/jira/browse/BEAM-2083) |
 - Contact: [Henning Rohde](mailto:herohde@google.com)

### Python 3 Support

Work is in progress to add Python 3 support to Beam.  Current goal is to make Beam codebase compatible both with Python 2.7 and Python 3.4.

 - [Proposal](https://docs.google.com/document/d/1xDG0MWVlDKDPu_IW9gtMvxi2S9I0GB0VDTkPhjXT0nE)
 - [Kanban Board](https://issues.apache.org/jira/secure/RapidBoard.jspa?rapidView=245&view=detail)

Contributions are welcome! If you are interested to help, you can select a subpackage to port and assign yourself the corresponding issue. Comment on the issue if you cannot assign it yourself.
When submitting a new PR, please tag [@RobbeSneyders](https://github.com/robbesneyders), [@aaltay](https://github.com/aaltay), and [@tvalentyn](https://github.com/tvalentyn).

### Next Java LTS version support (Java 11 / 18.9)

Work to support the next LTS release of Java is in progress. For more details about the scope and info on the various tasks please see the JIRA ticket.

- JIRA: [BEAM-2530](https://issues.apache.org/jira/issues/BEAM-2530)
- Contact: [Ismaël Mejía](mailto:iemejia@gmail.com)

### IO Performance Testing

We are also working on writing Performance Tests for IOs and developing a Performance Testing Framework for them. Contributions are welcome in the following areas:

 - developing more IO Performance Tests (IOITs)
 - providing necessary kubernetes infrastructure (eg. for databases or filesystems to be used in tests)
 - running Performance Tests on runners other than Dataflow and Direct
 - improving existing Performance Testing Framework and it's documentation

See the [documentation](https://beam.apache.org/documentation/io/testing/#i-o-transform-integration-tests) and the [initial proposal](https://docs.google.com/document/d/1dA-5s6OHiP_cz-NRAbwapoKF5MEC1wKps4A5tFbIPKE/edit?usp=sharing)(for file based tests).

If you're willing to help in this area, tag the following people in PRs: [@chamikaramj](https://github.com/chamikaramj), [@DariuszAniszewski](https://github.com/dariuszaniszewski), [@lgajowy](https://github.com/lgajowy), [@szewi](https://github.com/szewi), [@kkucharc](https://github.com/kkucharc)

### Euphoria Java 8 DSL

Easy to use Java 8 DSL for the Beam Java SDK. Provides a high-level abstraction of Beam transformations, which is both easy to read and write. Can be used as a complement to existing Beam pipelines (convertible back and forth). You can have a glimpse of the API at [WordCount example]({{ site.baseurl
}}/documentation/sdks/java/euphoria/#wordcount-example).

- Feature branch: [dsl-euphoria](https://github.com/apache/beam/tree/dsl-euphoria)
- JIRA: [dsl-euphoria](https://issues.apache.org/jira/browse/BEAM-4366?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20dsl-euphoria) / [BEAM-3900](https://issues.apache.org/jira/browse/BEAM-3900)
- Contact: [David Moravek](mailto:david.moravek@gmail.com)

### Improving the contributor experience

Making it easier to write code, run tests, and release. Investigating using docker for jenkins builds, automating the release process, and improving the reliability of tests.

Ideas and help welcome! Contact: [Alan Myrvold](mailto:amyrvold@google.com), [Mark Liu](mailto:markliu@google.com), [Yifan Zou](mailto:yifanzou@google.com)


### Beam SQL

Beam SQL has lots of areas to contribute: support for new operators, new
connectors, performance measurement and improvement, more full specification
and testing, etc.

 - JIRA: [dsl-sql](https://issues.apache.org/jira/issues/?filter=12343977)
 - Contact: [Kenneth Knowles](mailto:kenn@apache.org)

### Add benchmarks to continuous integration

Run Nexmark benchmark queries after each commit for Spark, Flink and Direct Runner and export response times to performance dashboards

- JIRA: [nexmark-perfkit](https://issues.apache.org/jira/browse/BEAM-4225)
- Contact: [Etienne Chauchot](mailto:echauchot@apache.org)

### Extract metrics in a runner agnostic way

Metrics are pushed by the runners to configurable sinks (HTTP REST sink available).
It is already enabled in Filnk and Spark runner. Work is in progress for Dataflow

- JIRA: [runner-agnostic-metrics](https://issues.apache.org/jira/browse/BEAM-3310)
- Contact: [Etienne Chauchot](mailto:echauchot@apache.org)

## Stale pull requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future. The associated JIRAs will be
unassigned from the author but will stay open.
