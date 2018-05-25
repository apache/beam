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

# Apache Beam Contribution Guide

The Apache Beam community welcomes contributions from anyone!

There are lots of opportunities:

 - write new examples
 - improve the documentation
 - add new user-facing libraries (new statistical libraries, new IO connectors,
   etc)
 - improve your favorite language SDK (Java, Python, Go, etc)
 - improve specific runners (Apache Apex, Apache Flink, Apache Spark, Google
   Cloud Dataflow, etc)
 - work on the core programming model (what is a Beam pipeline and how does it
   run?)

Most importantly, if you have an idea of how to contribute, then do it! 

For a list of open starter tasks, check
[https://s.apache.org/beam-starter-tasks](https://s.apache.org/beam-starter-tasks).

And, of course, we would love if you [contact us]({{ site.baseurl
}}/community/contact-us/) and introduce yourself.

## Contributing code

Coding happens at
[https://github.com/apache/beam](https://github.com/apache/beam). To
contribute, follow the usual GitHub process: fork the repo, make your changes,
and open a pull request. If you are unfamiliar with this workflow, GitHub
maintains these helpful guides:

 - [Git Handbook](https://guides.github.com/introduction/git-handbook/)
 - [Forking a repository](https://guides.github.com/activities/forking/)

If your change is large, it is a good idea to [discuss it on the dev@ mailing list]({{
site.baseurl }}/community/contact-us/). You will also
need to submit a signed [Individual Contributor License
Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
Software Foundation (ASF).  The purpose of this agreement is to clearly define
the terms under which intellectual property has been contributed to the ASF and
thereby allow us to defend the project should there be a legal dispute
regarding the software at some future time.

If you are contributing a `PTransform` to Beam, we have an extensive
[PTransform Style Guide]({{ site.baseurl }}/contribute/ptransform-style-guide).

### Building & Testing

We use Gradle to orchestrate building and testing.

The entire set of tests can be run with this command at the root of the git
repository.

    $ ./gradlew check

You can limit testing to a particular module and Gradle will build just the
necessary things to run those tests. For example:

    $ ./gradlew -p sdks/go check
    $ ./gradlew -p sdks/java/io/cassandra check
    $ ./gradlew -p runners/flink check

### Testing the Python SDK

You can directly use the Python toolchain instead of having Gradle orchestrate
it. This may be faster for you. We recommend setting up a virtual environment
before testing your code.

If you update any of the [cythonized](http://cython.org) files in Python SDK,
you must install the `cython` package before running following command to
properly test your code. 

The following commands should be run in the `sdks/python` directory.
This command runs all Python tests.

    $ python setup.py nosetests

You can use following command to run a single test method.

    $ python setup.py nosetests --tests <module>:<test class>.<test method>

    Example:
    $ python setup.py nosetests --tests apache_beam.io.textio_test:TextSourceTest.test_progress

To check just for lint errors, run the following command.

    $ ../../gradlew lint

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

## Stale pull requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future. The associated JIRAs will be
unassigned from the author but will stay open.
