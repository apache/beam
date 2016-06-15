---
layout: post
title:  "The first release of Apache Beam!"
date:   2016-06-15 00:00:01 -0700
excerpt_separator: <!--more-->
categories: beam release
authors:
  - davor
---

I’m happy to announce that Apache Beam has officially released its first
version -- 0.1.0-incubating. This is an exciting milestone for the project,
which joined the Apache Software Foundation and the Apache Incubator earlier
this year.

<!--more-->

This release publishes the first set of Apache Beam binaries and source code,
making them readily available for our users. The initial release includes the
SDK for Java, along with three runners: Apache Flink, Apache Spark and Google
Cloud Dataflow, a fully-managed cloud service. The release is available both
in the [Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22),
as well as a download from the [project’s website]({{ site.baseurl }}/releases/).

The goal of this release was process-oriented. In particular, the Beam
community wanted to release existing functionality to our users, build and
validate the release processes, and obtain validation from the Apache Software
Foundation and the Apache Incubator.

I’d like to encourage everyone to try out this release. Please keep in mind
that this is the first incubating release -- significant changes are to be
expected. As we march toward stability, a rapid cadence of future releases is
anticipated, perhaps one every 1-2 months.

As always, the Beam community welcomes feedback. Stabilization, usability and
the developer experience will be our focus for the next several months. If you
have any comments or discover any issues, I’d like to invite you to reach out
to us via [user’s mailing list]({{ site.baseurl }}/mailing_lists/) or the
[Apache JIRA issue tracker](https://issues.apache.org/jira/browse/BEAM/).
