---
title: "Beam Contribution Guide"
type: "contribute"
layout: "arrow_list_template"
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

<div class="arrow-lists-desktop">

## Overview

There are lots of opportunities to contribute. You can for example:

- ask or answer questions on [user@beam.apache.org](/community/contact-us/) or
  [stackoverflow](https://stackoverflow.com/questions/tagged/apache-beam)
- review proposed design ideas on [dev@beam.apache.org](/community/contact-us/)
- file [bug reports](https://issues.apache.org/jira/projects/BEAM/issues)
- review [changes](https://github.com/apache/beam/pulls)
- work on the core programming model (what is a Beam pipeline and how does it
  run?)
- improve the developer experience (for example, Windows guides)
- organize local meetups of users or contributors to Apache Beam

...and many more. Most importantly, if you have an idea of how to contribute, then do it!

## Contributing code

Below is a tutorial for contributing code to Beam, covering our tools and typical process in
detail.

### Prerequisites

- a GitHub account
- a Linux, macOS, or Microsoft Windows development environment with Java JDK 8 installed
- [Docker](https://www.docker.com/) installed for some tasks including building worker containers and testing this website
  changes locally
- [Go](https://golang.org) 1.12 or later installed for Go SDK development
- Python 3.6, 3.7, and 3.8. Yes, you need all four versions installed.

  pip, setuptools, virtualenv, and tox installed for Python development

- for large contributions, a signed [Individual Contributor License
  Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
  Software Foundation (ASF).

</br>

```
sudo apt-get install \
   openjdk-8-jdk \
   python-setuptools \
   python-pip \
   virtualenv \
   tox \
   docker-ce
```

</br>

You also need to [install Go](https://golang.org/doc/install).

</br>

Once Go is installed, install goavro:

</br>

```
$ export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
$ go get github.com/linkedin/goavro
```

</br>

Linux users should configure their machines for sudoless Docker.

</div>

<div class="arrow-lists-mobile">

 <a class="arrow-list-header" data-toggle="collapse" href="#collapseOverview" role="button" aria-expanded="false" aria-controls="collapseOverview">
  {{< figure src="/images/arrow-icon_list.svg">}}

Overview

  </a>

<div class="collapse" id="collapseOverview">

There are lots of opportunities to contribute. You can for example:

- ask or answer questions on [user@beam.apache.org](/community/contact-us/) or
  [stackoverflow](https://stackoverflow.com/questions/tagged/apache-beam)
- review proposed design ideas on [dev@beam.apache.org](/community/contact-us/)
- file [bug reports](https://issues.apache.org/jira/projects/BEAM/issues)
- review [changes](https://github.com/apache/beam/pulls)
- work on the core programming model (what is a Beam pipeline and how does it
  run?)
- improve the developer experience (for example, Windows guides)
- organize local meetups of users or contributors to Apache Beam

...and many more. Most importantly, if you have an idea of how to contribute, then do it!

</div>

<a class="arrow-list-header" data-toggle="collapse" href="#collapseContributing" role="button" aria-expanded="false" aria-controls="collapseContributing">
   {{< figure src="/images/arrow-icon_list.svg">}}

Contributing code

  </a>

<div class="collapse" id="collapseContributing">

Below is a tutorial for contributing code to Beam, covering our tools and typical process in
detail.

- a GitHub account
- a Linux, macOS, or Microsoft Windows development environment with Java JDK 8 installed
- [Docker](https://www.docker.com/) installed for some tasks including building worker containers and testing this website
  changes locally
- [Go](https://golang.org) 1.12 or later installed for Go SDK development
- Python 3.6, 3.7, and 3.8. Yes, you need all four versions installed.

  pip, setuptools, virtualenv, and tox installed for Python development

- for large contributions, a signed [Individual Contributor License
  Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA) to the Apache
  Software Foundation (ASF).

</div>

</br>

```
sudo apt-get install \
   openjdk-8-jdk \
   python-setuptools \
   python-pip \
   virtualenv \
   tox \
   docker-ce
```

</br>

You also need to [install Go](https://golang.org/doc/install).

</br>

Once Go is installed, install goavro:

</br>

```
$ export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
$ go get github.com/linkedin/goavro
```

</br>

Linux users should configure their machines for sudoless Docker.

</div>

### Connect With the Beam community

- Consider subscribing to the [dev@ mailing list](/community/contact-us/), especially
  if you plan to make more than one change or the change will be large. All decisions happen on the
  public dev list.
- (Optionally) Join the [#beam channel of the ASF slack](/community/contact-us/).
- Create an account on [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues)
  (anyone can do this).

### Share your intent

- Find or create an issue in the [Beam issue tracker (JIRA)](https://issues.apache.org/jira/projects/BEAM/issues).
  Tracking your work in an issue will avoid duplicated or conflicting work, and provide
  a place for notes. Later, your pull request will be linked to the issue as well.
- If you want to get involved but don't have a project in mind, check our list of open starter tasks,
  [https://s.apache.org/beam-starter-tasks](https://s.apache.org/beam-starter-tasks).
- Assign the issue to yourself. To get the permission to do so, email
  the [dev@ mailing list](/community/contact-us)
  to introduce yourself and to be added as a contributor in the Beam issue tracker including your
  ASF Jira Username. For example [this welcome email](https://lists.apache.org/thread.html/e6018c2aaf7dc7895091434295e5b0fafe192b975e3e3761fcf0cda7@%3Cdev.beam.apache.org%3E).
- If your change is large or it is your first change, it is a good idea to
  [discuss it on the dev@ mailing list](/community/contact-us/).
- For large changes create a design doc
  ([template](https://s.apache.org/beam-design-doc-template),
  [examples](https://s.apache.org/beam-design-docs)) and email it to the [dev@ mailing list](/community/contact-us).
