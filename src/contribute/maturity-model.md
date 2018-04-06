---
layout: section
title: "Apache Maturity Model Assessment for Apache Beam"
section_menu: section-menu/contribute.html
permalink: /contribute/maturity-model/
---

# Apache Maturity Model Assessment for Apache Beam

*Apache Beam has graduated from incubation as a top-level project at the
Apache Software Foundation. This page was last updated as a part of the
graduation process and is no longer being maintained.*

* TOC
{:toc}

## Maturity model
The following table summarizes project's self-assessment against the Apache Maturity Model.

<table class="table table-hover">
  <thead>
    <tr>
      <th>ID</th>
      <th>Description</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td colspan="3"><p><b><i>Code</i></b></p></td>
    </tr>
    <tr>
      <td><p>CD10</p></td>
      <td><p>The project produces Open Source software, for distribution to the public at no charge. [1]</p></td>
      <td><p><b>YES.</b> The project source code is <a href="https://github.com/apache/beam/blob/master/LICENSE">licensed</a> under the Apache License, version 2.0.</p></td>
    </tr>
    <tr>
      <td><p>CD20</p></td>
      <td><p>The project&#39;s code is easily discoverable and publicly accessible.</p></td>
      <td><p><b>YES.</b> Linked from the <a href="{{ site.baseurl }}/contribute/source-repository/">website</a>, available via <a href="https://gitbox.apache.org/repos/asf?p%3Dbeam.git">git.apache.org</a> and <a href="https://github.com/apache/beam">GitHub</a>.</p></td>
    </tr>
    <tr>
      <td><p>CD30</p></td>
      <td><p>The code can be built in a reproducible way using widely available standard tools.</p></td>
      <td><p><b>YES.</b> The project uses Gradle and can be built via <a href="https://github.com/apache/beam/blob/master/README.md">the standard &ldquo;gradle build&rdquo; on any platform</a>.</p></td>
    </tr>
    <tr>
      <td><p>CD40</p></td>
      <td><p>The full history of the project&#39;s code is available via a source code control system, in a way that allows any released version to be recreated.</p></td>
      <td><p><b>YES.</b> The project uses a <a href="https://github.com/apache/beam">git repository</a> and releases are <a href="https://github.com/apache/beam/releases">tagged</a>.</p><p><b>Even further</b>, all release candidates are <a href="https://github.com/apache/beam/releases">tagged</a> as well.</p></td>
    </tr>
    <tr>
      <td><p>CD50</p></td>
      <td><p>The provenance of each line of code is established via the source code control system, in a reliable way based on strong authentication of the committer. When third-party contributions are committed, commit messages provide reliable information about the code provenance. [2]</p></td>
      <td><p><b>YES.</b> The project uses a <a href="https://github.com/apache/beam">git repository</a>, managed by Apache Infra, ensuring provenance of each line of code to a committer. Third party contributions are accepted in accordance with the <a href="{{ site.baseurl }}/contribute/contribution-guide/">Contribution Guide</a> only.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Licenses and Copyright</i></b></p></td>
    </tr>
    <tr>
      <td><p>LC10</p></td>
      <td><p>The code is released under the Apache License, version 2.0.</p></td>
      <td><p><b>YES.</b> Source distributions clearly state <a href="https://github.com/apache/beam/blob/master/LICENSE">license</a>. Convenience binaries clearly state <a href="https://github.com/apache/beam/blob/master/pom.xml#L41">license</a>.</p></td>
    </tr>
    <tr>
      <td><p>LC20</p></td>
      <td><p>Libraries that are mandatory dependencies of the project&#39;s code do not create more restrictions than the Apache License does. [3], [4]</p></td>
      <td><p><b>YES.</b> The list of mandatory dependencies have been reviewed to contain approved licenses only. See below.</p></td>
    </tr>
    <tr>
      <td><p>LC30</p></td>
      <td><p>The libraries mentioned in LC20 are available as Open Source software.</p></td>
      <td><p><b>YES.</b> All mandatory dependencies are available as open source software. See below.</p></td>
    </tr>
    <tr>
      <td><p>LC40</p></td>
      <td><p>Committers are bound by an Individual Contributor Agreement (the &quot;Apache iCLA&quot;) that defines which code they are allowed to commit and how they need to identify code that is not their own.</p></td>
      <td><p><b>YES.</b> The project uses a repository managed by Apache Infra -- write access requires an Apache account, which requires an ICLA on file.</p><p><b>Even further</b>, the <a href="{{ site.baseurl }}/contribute/contribution-guide/">contribution guide</a> requires ICLA for &ldquo;larger&rdquo; contributions in the short span before they officially become committers.</p></td>
    </tr>
    <tr>
      <td><p>LC50</p></td>
      <td><p>The copyright ownership of everything that the project produces is clearly defined and documented. [5]</p></td>
      <td><p><b>YES.</b> All files in the source repository have appropriate headers.</p><p><b>Even further</b>, Software Grant Agreements for the initial donations and Corporate CLAs have been filed.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Releases</i></b></p></td>
    </tr>
    <tr>
      <td><p>RE10</p></td>
      <td><p>Releases consist of source code, distributed using standard and open archive formats that are expected to stay readable in the long term. [6]</p></td>
      <td><p><b>YES.</b> <a href="https://dist.apache.org/repos/dist/release/beam/">Source releases</a> are distributed via dist.apache.org and linked from the <a href="{{ site.baseurl }}/get-started/downloads/">website</a>.</p></td>
    </tr>
    <tr>
      <td><p>RE20</p></td>
      <td><p>Releases are approved by the project&#39;s PMC (see CS10), in order to make them an act of the Foundation.</p></td>
      <td><p><b>YES.</b> All incubating releases have been unanimously approved by the Beam community and the Incubator, all with at least 3 (P)PMC votes.</p></td>
    </tr>
    <tr>
      <td><p>RE30</p></td>
      <td><p>Releases are signed and/or distributed along with digests that can be reliably used to validate the downloaded archives.</p></td>
      <td><p><b>YES.</b> All releases are signed, and the <a href="https://dist.apache.org/repos/dist/release/beam/KEYS">KEYS file</a> is provided on dist.apache.org.</p></td>
    </tr>
    <tr>
      <td><p>RE40</p></td>
      <td><p>Convenience binaries can be distributed alongside source code but they are not Apache Releases -- they are just a convenience provided with no guarantee.</p></td>
      <td><p><b>YES.</b> Convenience binaries are distributed via <a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3Aorg.apache.beam">Maven Central Repository</a> only. They are not distributed via dist.apache.org.</p></td>
    </tr>
    <tr>
      <td><p>RE50</p></td>
      <td><p>The release process is documented and repeatable to the extent that someone new to the project is able to independently generate the complete set of artifacts required for a release.</p></td>
      <td><p><b>YES.</b> A very detailed <a href="{{ site.baseurl }}/contribute/release-guide/">release guide</a> is available on the website.</p><p><b>Even further</b>, each incubating release was driven by a different release manager, across multiple organizations.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Quality</i></b></p></td>
    </tr>
    <tr>
      <td><p>QU10</p></td>
      <td><p>The project is open and honest about the quality of its code. Various levels of quality and maturity for various modules are natural and acceptable as long as they are clearly communicated.</p></td>
      <td><p><b>YES.</b> The project records all bugs in the <a href="https://issues.apache.org/jira/browse/BEAM">Apache&rsquo;s JIRA issue tracker</a>.</p><p><b>Even further</b>, the project <a href="{{ site.baseurl }}/get-started/downloads/">clearly documents</a> that APIs are subject to change before the first stable release, and which APIs are <a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/annotations/Experimental.java#L47">@Experimental</a>.</p></td>
    </tr>
    <tr>
      <td><p>QU20</p></td>
      <td><p>The project puts a very high priority on producing secure software. [7]</p></td>
      <td><p><b>YES.</b> Security issues are treated with the highest priority.</p></td>
    </tr>
    <tr>
      <td><p>QU30</p></td>
      <td><p>The project provides a well-documented channel to report security issues, along with a documented way of responding to them. [8]</p></td>
      <td><p><b>YES.</b> The <a href="{{ site.baseurl }}/">website</a> provides a link to the ASF security information (in the ASF dropdown menu).</p></td>
    </tr>
    <tr>
      <td><p>QU40</p></td>
      <td><p>The project puts a high priority on backwards compatibility and aims to document any incompatible changes and provide tools and documentation to help users transition to new features.</p></td>
      <td><p><b>YES.</b> <a href="{{ site.baseurl }}/get-started/downloads/">Backward compatibility guarantees</a> are documented on the website. Once we reach the first stable release, the project <a href="https://lists.apache.org/thread.html/52f27bd52239fa2efe054b2c1baad658fc6de16e2427b52448b3cf15@%253Cuser.beam.apache.org%253E">aims to make no backward incompatible changes</a> within a given major version. In addition, <a href="{{ site.baseurl }}/get-started/downloads/">release notes</a> are provided for each release.</p></td>
    </tr>
    <tr>
      <td><p>QU50</p></td>
      <td><p>The project strives to respond to documented bug reports in a timely manner.</p></td>
      <td><p><b>YES.</b> The project has resolved <a href="https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20status%20in%20(Resolved%2C%20Closed)">550 issues</a> during incubation.</p><p><b>Even further</b>, <a href="https://issues.apache.org/jira/projects/BEAM?selectedItem=com.atlassian.jira.jira-projects-plugin%3Acomponents-page&selectedTab%3Dcom.atlassian.jira.jira-projects-plugin%3Acomponents-panel=undefined">all project components</a> have designated a single committer who gets assigned all newly filed issues for a triage/re-assignment to ensure timely action.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Community</i></b></p></td>
    </tr>
    <tr>
      <td><p>CO10</p></td>
      <td><p>The project has a well-known homepage that points to all the information required to operate according to this maturity model.</p></td>
      <td><p><b>YES.</b> The project <a href="{{ site.baseurl }}/">website</a> has <a href="https://goo.gl/nk5OM0">technical vision</a>, <a href="{{ site.baseurl }}/contribute/contribution-guide/">contribution guide</a>, <a href="{{ site.baseurl }}/contribute/release-guide/">release guide</a>, <a href="{{ site.baseurl }}/contribute/testing/">testing guide</a>, <a href="{{ site.baseurl }}/contribute/design-principles/">design principles</a>, <a href="{{ site.baseurl }}/contribute/work-in-progress/">a list of major efforts</a>, and links to <a href="{{ site.baseurl }}/get-started/support/">mailing lists</a>, <a href="{{ site.baseurl }}/contribute/source-repository/">repositories</a>, <a href="{{ site.baseurl }}/get-started/support/">issue tracker</a>.</p></td>
    </tr>
    <tr>
      <td><p>CO20</p></td>
      <td><p>The community welcomes contributions from anyone who acts in good faith and in a respectful manner and adds value to the project.</p></td>
      <td><p><b>YES.</b> This is a part of the <a href="{{ site.baseurl }}/contribute/contribution-guide/">contribution guide</a>. Many introductory emails available as evidence, e.g., <a href="https://lists.apache.org/thread.html/8c46c41d432edd5c6c6fa9942d95ffead24ab42a64c785de21fc18c0@%253Cdev.beam.apache.org%253E">this</a>, <a href="https://lists.apache.org/thread.html/272281cf15482fedf39d02f66423cba858a3731914b6b05009c58225@%3Cdev.beam.apache.org%3E">that</a>, and <a href="https://lists.apache.org/thread.html/f7fdd33e8984d4e4b84483beee06aaba55438caa97f63121729f7a6b@%3Cdev.beam.apache.org%3E">the other</a>. Many <a href="https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20labels%20%3D%20starter">starter tasks</a> available in issue tracker. See also community growth statistics below.</p></td>
    </tr>
    <tr>
      <td><p>CO30</p></td>
      <td><p>Contributions include not only source code, but also documentation, constructive bug reports, constructive discussions, marketing and generally anything that adds value to the project.</p></td>
      <td><p><b>YES.</b> The <a href="{{ site.baseurl }}/contribute/contribution-guide/">contribution guide</a> specifically calls out many avenues for contribution. Specifically, several contributors have contributed documentation only.</p><p><b>Even further</b>, the community has elected a committer mostly based on the evangelization work.</p></td>
    </tr>
    <tr>
      <td><p>CO40</p></td>
      <td><p>The community is meritocratic and over time aims to give more rights and responsibilities to contributors who add value to the project.</p></td>
      <td><p><b>YES.</b> The community has elected 3 new committers during incubation.</p></td>
    </tr>
    <tr>
      <td><p>CO50</p></td>
      <td><p>The way in which contributors can be granted more rights such as commit access or decision power is clearly documented and is the same for all contributors.</p></td>
      <td><p><b>YES.</b> The criteria is documented in the <a href="{{ site.baseurl }}/contribute/contribution-guide/#granting-more-rights-to-a-contributor">contribution guide</a>.</p></td>
    </tr>
    <tr>
      <td><p>CO60</p></td>
      <td><p>The community operates based on consensus of its members (see CS10) who have decision power. Dictators, benevolent or not, are not welcome in Apache projects.</p></td>
      <td><p><b>YES.</b> The project works to build consensus. All votes have been unanimous so far. Votes on technical decision were rarely needed, if ever, as the consensus was already apparent.</p></td>
    </tr>
    <tr>
      <td><p>CO70</p></td>
      <td><p>The project strives to answer user questions in a timely manner.</p></td>
      <td><p><b>YES.</b> The project typically provides detailed answers to user questions within a few hours via <a href="https://lists.apache.org/list.html?user@beam.apache.org">user@ mailing list</a>.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Consensus Building</i></b></p></td>
    </tr>
    <tr>
      <td><p>CS10</p></td>
      <td><p>The project maintains a public list of its contributors who have decision power -- the project&#39;s PMC (Project Management Committee) consists of those contributors.</p></td>
      <td><p><b>YES.</b> The website contains the <a href="{{ site.baseurl }}/contribute/team/">list of committers and PPMC members</a>.</p></td>
    </tr>
    <tr>
      <td><p>CS20</p></td>
      <td><p>Decisions are made by consensus among PMC members [9] and are documented on the project&#39;s main communications channel. Community opinions are taken into account but the PMC has the final word if needed.</p></td>
      <td><p><b>YES.</b> The project has been making important decisions on the project mailing lists. Vast majority of, if not all, decisions have had a consensus without any PPMC action needed.</p></td>
    </tr>
    <tr>
      <td><p>CS30</p></td>
      <td><p>Documented voting rules are used to build consensus when discussion is not sufficient. [10]</p></td>
      <td><p><b>YES.</b> The project uses the standard ASF voting rules. Voting rules are clearly stated before the voting starts for each individual vote.</p></td>
    </tr>
    <tr>
      <td><p>CS40</p></td>
      <td><p>In Apache projects, vetoes are only valid for code commits and are justified by a technical explanation, as per the Apache voting rules defined in CS30.</p></td>
      <td><p><b>YES.</b> The project hasn&rsquo;t used a veto at any point and relies on robust code reviews.</p></td>
    </tr>
    <tr>
      <td><p>CS50</p></td>
      <td><p>All &quot;important&quot; discussions happen asynchronously in written form on the project&#39;s main communications channel. Offline, face-to-face or private discussions [11] that affect the project are also documented on that channel.</p></td>
      <td><p><b>YES.</b> The project has been making important decisions on the project mailing lists. Minor decisions may occasionally happen during code reviews, which are also asynchronous and in written form.</p></td>
    </tr>

    <tr>
      <td colspan="3"><p><b><i>Independence</i></b></p></td>
    </tr>
    <tr>
      <td><p>IN10</p></td>
      <td><p>The project is independent from any corporate or organizational influence. [12]</p></td>
      <td><p><b>YES.</b> See below.</p></td>
    </tr>
    <tr>
      <td><p>IN20</p></td>
      <td><p>Contributors act as themselves as opposed to representatives of a corporation or organization.</p></td>
      <td><p><b>YES.</b> See below.</p></td>
    </tr>
  </tbody>
</table>

#### Footnotes
[1] "For distribution to the public at no charge" is straight from the from the ASF Bylaws at http://apache.org/foundation/bylaws.html.

[2] See also LC40.

[3] It's ok for platforms (like a runtime used to execute our code) to have different licenses as long as they don't impose reciprocal licensing on what we are distributing.

[4] http://apache.org/legal/resolved.html has information about acceptable licenses for third-party dependencies

[5] In Apache projects, the ASF owns the copyright for the collective work, i.e. the project's releases. Contributors retain copyright on their contributions but grant the ASF a perpetual copyright license for them.

[6] See http://www.apache.org/dev/release.html for more info on Apache releases

[7] The required level of security depends on the software's intended uses, of course. Expectations should be clearly documented.

[8] Apache projects can just point to http://www.apache.org/security/ or use their own security contacts page, which should also point to that.

[9] In Apache projects, "consensus" means widespread agreement among people who have decision power. It does not necessarily mean "unanimity".

[10] For Apache projects, http://www.apache.org/foundation/voting.html defines the voting rules.

[11] Apache projects have a private mailing list that their PMC is expected to use only when really needed. The private list is typically used for discussions about people, for example to discuss and to vote on PMC candidates privately.

[12] Independence can be understood as basing the project's decisions on the open discussions that happen on the project's main communications channel, with no hidden agendas.

## Independence / Community
During incubation, the Beam community has worked hard to remove any special treatment given to any organization. The bulk of the initial code donation came from Google's Cloud Dataflow SDK, and this provenance was originally visible throughout the code in naming, package organization, and assumptions about execution environment. After a significant amount of refactoring, both the project's code and branding treat all 'runners' equally and have a clean separation between the project and Google Cloud Dataflow (now just one of many runners that can be used within Beam).

While the majority of commits is still provided by a single organization, it is interesting to look the distribution per-module. Out of the ~22 large modules in the codebase, at least 10 modules have been developed from scratch by the community, with little to no contribution from the largest organization.

Finally, the contributor diversity has increased significantly. Over each of the last three months, no organization has had more than ~50% of the unique contributors per month. (Assumptions: commits to master branch of the main repository, excludes merge commits, best effort to identify unique contributors).

![Contributor diversity graph](
  {{ "/images/contribution-diversity.png" | prepend: site.baseurl }})

## Dependency analysis
This section analyses project's direct and transitive dependencies to ensure compliance with Apache Software Foundation's policies and guidelines.

### License analysis
The following is a list of licenses for all direct and transitive dependencies. The list is annotated where applicable.
```
Lists of 397 third-party dependencies.
     (Public Domain) AOP alliance (aopalliance:aopalliance:1.0 - http://aopalliance.sourceforge.net)
     (Unknown license) ASM Core (asm:asm:3.1 - http://asm.objectweb.org/asm/)
         Actual license: BSD License.
     (The Apache Software License, Version 2.0) ZkClient (com.101tec:zkclient:0.3 - https://github.com/sgroschupf/zkclient)
     (The Apache Software License, Version 2.0) ZkClient (com.101tec:zkclient:0.7 - https://github.com/sgroschupf/zkclient)
     (Amazon Software License) Amazon Kinesis Client Library for Java (com.amazonaws:amazon-kinesis-client:1.6.1 - https://aws.amazon.com/kinesis)
         Optional dependency: OK.
     (Apache License, Version 2.0) AWS Java SDK for Amazon CloudWatch (com.amazonaws:aws-java-sdk-cloudwatch:1.10.20 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) AWS SDK for Java - Core (com.amazonaws:aws-java-sdk-core:1.11.18 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) AWS Java SDK for Amazon DynamoDB (com.amazonaws:aws-java-sdk-dynamodb:1.10.20 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) AWS Java SDK for Amazon Kinesis (com.amazonaws:aws-java-sdk-kinesis:1.11.18 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) AWS Java SDK for AWS KMS (com.amazonaws:aws-java-sdk-kms:1.10.20 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) AWS Java SDK for Amazon S3 (com.amazonaws:aws-java-sdk-s3:1.10.20 - https://aws.amazon.com/sdkforjava)
     (Apache License, Version 2.0) stream-lib (com.clearspring.analytics:stream:2.7.0 - https://github.com/addthis/stream-lib)
     (The Apache License, Version 2.0) Netlet (com.datatorrent:netlet:1.2.1 - http://www.datatorrent.com/product/netlet)
     (New BSD License) Kryo (com.esotericsoftware.kryo:kryo:2.21 - http://code.google.com/p/kryo/)
     (New BSD License) Kryo (com.esotericsoftware.kryo:kryo:2.24.0 - https://github.com/EsotericSoftware/kryo)
     (New BSD License) MinLog (com.esotericsoftware.minlog:minlog:1.2 - http://code.google.com/p/minlog/)
     (New BSD License) ReflectASM (com.esotericsoftware.reflectasm:reflectasm:1.07 - http://code.google.com/p/reflectasm/)
     (The Apache Software License, Version 2.0) Jackson-annotations (com.fasterxml.jackson.core:jackson-annotations:2.7.2 - http://github.com/FasterXML/jackson)
     (The Apache Software License, Version 2.0) Jackson-core (com.fasterxml.jackson.core:jackson-core:2.7.2 - https://github.com/FasterXML/jackson-core)
     (The Apache Software License, Version 2.0) jackson-databind (com.fasterxml.jackson.core:jackson-databind:2.7.2 - http://github.com/FasterXML/jackson)
     (The Apache Software License, Version 2.0) Jackson-dataformat-CBOR (com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.6.6 - http://wiki.fasterxml.com/JacksonForCbor)
     (The Apache Software License, Version 2.0) Jackson-module-paranamer (com.fasterxml.jackson.module:jackson-module-paranamer:2.7.2 - http://wiki.fasterxml.com/JacksonHome)
     (The Apache Software License, Version 2.0) jackson-module-scala (com.fasterxml.jackson.module:jackson-module-scala_2.10:2.7.2 - http://wiki.fasterxml.com/JacksonModuleScala)
     (MIT License) scopt (com.github.scopt:scopt_2.10:3.2.0 - https://github.com/scopt/scopt)
     (Apache License, Version 2.0) named-regexp (com.github.tony19:named-regexp:0.2.3 - http://nexus.sonatype.org/oss-repository-hosting.html/named-regexp)
     (The Apache Software License, Version 2.0) Google APIs Client Library for Java (com.google.api-client:google-api-client:1.22.0 - https://github.com/google/google-api-java-client/google-api-client)
     (The Apache Software License, Version 2.0) Jackson 2 extensions to the Google APIs Client Library for Java (com.google.api-client:google-api-client-jackson2:1.22.0 - https://github.com/google/google-api-java-client/google-api-client-jackson2)
     (The Apache Software License, Version 2.0) Java 6 (and higher) Extensions to the Google API Client Library for Java. (com.google.api-client:google-api-client-java6:1.22.0 - https://github.com/google/google-api-java-client/google-api-client-java6)
     (Apache-2.0) com.google.api.grpc:grpc-google-common-protos (com.google.api.grpc:grpc-google-common-protos:0.1.0 - https://github.com/googleapis/googleapis)
     (Apache-2.0) com.google.api.grpc:grpc-google-iam-v1 (com.google.api.grpc:grpc-google-iam-v1:0.1.0 - https://github.com/googleapis/googleapis)
     (Apache-2.0) com.google.api.grpc:grpc-google-pubsub-v1 (com.google.api.grpc:grpc-google-pubsub-v1:0.1.0 - https://github.com/googleapis/googleapis)
     (The Apache Software License, Version 2.0) BigQuery API v2-rev295-1.22.0 (com.google.apis:google-api-services-bigquery:v2-rev295-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-bigquery)
     (The Apache Software License, Version 2.0) Google Cloud Debugger API v2-rev8-1.22.0 (com.google.apis:google-api-services-clouddebugger:v2-rev8-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-clouddebugger)
     (The Apache Software License, Version 2.0) Google Cloud Resource Manager API v1-rev6-1.22.0 (com.google.apis:google-api-services-cloudresourcemanager:v1-rev6-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-cloudresourcemanager)
     (The Apache Software License, Version 2.0) Google Dataflow API v1b3-rev43-1.22.0 (com.google.apis:google-api-services-dataflow:v1b3-rev43-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-dataflow)
     (The Apache Software License, Version 2.0) Google Cloud Pub/Sub API v1-rev10-1.22.0 (com.google.apis:google-api-services-pubsub:v1-rev10-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-pubsub)
     (The Apache Software License, Version 2.0) Cloud Storage JSON API v1-rev71-1.22.0 (com.google.apis:google-api-services-storage:v1-rev71-1.22.0 - http://nexus.sonatype.org/oss-repository-hosting.html/google-api-services-storage)
     (Google App Engine Terms of Service) appengine-api-1.0-sdk (com.google.appengine:appengine-api-1.0-sdk:1.9.34 - http://code.google.com/appengine/appengine-api-1.0-sdk/)
     (BSD New license) Google Auth Library for Java - Google App Engine (com.google.auth:google-auth-library-appengine:0.4.0 - https://github.com/google/google-auth-library-java/google-auth-library-appengine)
     (BSD New license) Google Auth Library for Java - Credentials (com.google.auth:google-auth-library-credentials:0.6.0 - https://github.com/google/google-auth-library-java/google-auth-library-credentials)
     (BSD New license) Google Auth Library for Java - OAuth2 HTTP (com.google.auth:google-auth-library-oauth2-http:0.6.0 - https://github.com/google/google-auth-library-java/google-auth-library-oauth2-http)
     (Apache 2.0) Auto Common Libraries (com.google.auto:auto-common:0.3 - https://github.com/google/auto/auto-common)
     (Apache 2.0) AutoService (com.google.auto.service:auto-service:1.0-rc2 - https://github.com/google/auto/auto-service)
     (Apache 2.0) AutoValue (com.google.auto.value:auto-value:1.1 - https://github.com/google/auto/auto-value)
     (Apache License, Version 2.0) gcsio.jar (com.google.cloud.bigdataoss:gcsio:1.4.5 - https://github.com/GoogleCloudPlatform/BigData-interop/gcsio/)
     (Apache License, Version 2.0) util (com.google.cloud.bigdataoss:util:1.4.5 - https://github.com/GoogleCloudPlatform/BigData-interop/util/)
     (The Apache License, Version 2.0) com.google.cloud.bigtable:bigtable-client-core (com.google.cloud.bigtable:bigtable-client-core:0.9.2 - https://cloud.google.com/bigtable/bigtable-client-core-parent/bigtable-client-core/)
     (The Apache License, Version 2.0) com.google.cloud.bigtable:bigtable-protos (com.google.cloud.bigtable:bigtable-protos:0.9.2 - https://cloud.google.com/bigtable/bigtable-client-core-parent/bigtable-protos/)
     (Apache License, Version 2.0) Google Cloud Dataflow Java Proto Library - All (com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:0.5.160304 - http://cloud.google.com/dataflow)
     (The Apache License, Version 2.0) com.google.cloud.datastore:datastore-v1-proto-client (com.google.cloud.datastore:datastore-v1-proto-client:1.2.0 - https://cloud.google.com/datastore/datastore-v1-proto-client/)
     (The Apache License, Version 2.0) com.google.cloud.datastore:datastore-v1-protos (com.google.cloud.datastore:datastore-v1-protos:1.2.0 - https://cloud.google.com/datastore/)
     (The Apache Software License, Version 2.0) FindBugs-jsr305 (com.google.code.findbugs:jsr305:1.3.9 - http://findbugs.sourceforge.net/)
     (The Apache Software License, Version 2.0) FindBugs-jsr305 (com.google.code.findbugs:jsr305:3.0.1 - http://findbugs.sourceforge.net/)
     (The Apache Software License, Version 2.0) Gson (com.google.code.gson:gson:2.2.4 - http://code.google.com/p/google-gson/)
     (The Apache Software License, Version 2.0) Gson (com.google.code.gson:gson:2.3 - http://code.google.com/p/google-gson/)
     (Unknown license) error-prone annotations (com.google.errorprone:error_prone_annotations:2.0.2 - http://nexus.sonatype.org/oss-repository-hosting.html/error_prone_parent/error_prone_annotations)
     (The Apache Software License, Version 2.0) Guava: Google Core Libraries for Java (com.google.guava:guava:19.0 - https://github.com/google/guava/guava)
     (The Apache Software License, Version 2.0) Guava Testing Library (com.google.guava:guava-testlib:19.0 - https://github.com/google/guava/guava-testlib)
     (The Apache Software License, Version 2.0) Google HTTP Client Library for Java (com.google.http-client:google-http-client:1.22.0 - https://github.com/google/google-http-java-client/google-http-client)
     (The Apache Software License, Version 2.0) Jackson extensions to the Google HTTP Client Library for Java. (com.google.http-client:google-http-client-jackson:1.22.0 - https://github.com/google/google-http-java-client/google-http-client-jackson)
     (The Apache Software License, Version 2.0) Jackson 2 extensions to the Google HTTP Client Library for Java. (com.google.http-client:google-http-client-jackson2:1.22.0 - https://github.com/google/google-http-java-client/google-http-client-jackson2)
     (The Apache Software License, Version 2.0) Protocol Buffer extensions to the Google HTTP Client Library for Java. (com.google.http-client:google-http-client-protobuf:1.22.0 - https://github.com/google/google-http-java-client/google-http-client-protobuf)
     (The Apache Software License, Version 2.0) Google Guice - Core Library (com.google.inject:guice:3.0 - http://code.google.com/p/google-guice/guice/)
     (The Apache Software License, Version 2.0) Google Guice - Extensions - Servlet (com.google.inject.extensions:guice-servlet:3.0 - http://code.google.com/p/google-guice/extensions-parent/guice-servlet/)
     (The Apache Software License, Version 2.0) Google OAuth Client Library for Java (com.google.oauth-client:google-oauth-client:1.22.0 - https://github.com/google/google-oauth-java-client/google-oauth-client)
     (The Apache Software License, Version 2.0) Java 6 (and higher) extensions to the Google OAuth Client Library for Java. (com.google.oauth-client:google-oauth-client-java6:1.22.0 - https://github.com/google/google-oauth-java-client/google-oauth-client-java6)
     (New BSD license) Protocol Buffers [Core] (com.google.protobuf:protobuf-java:3.0.0 - https://developers.google.com/protocol-buffers/protobuf-java/)
     (New BSD license) Protocol Buffers [Util] (com.google.protobuf:protobuf-java-util:3.0.0 - https://developers.google.com/protocol-buffers/protobuf-java-util/)
     (New BSD license) Protocol Buffers [Lite] (com.google.protobuf:protobuf-lite:3.0.1 - https://developers.google.com/protocol-buffers/protobuf-lite/)
     (New BSD license) Protocol Buffer JavaNano API (com.google.protobuf.nano:protobuf-javanano:3.0.0-alpha-5 - https://developers.google.com/protocol-buffers/)
     (Apache License, Version 2.0) java-xmlbuilder (com.jamesmurty.utils:java-xmlbuilder:0.4 - http://code.google.com/p/java-xmlbuilder/)
     (BSD) JSch (com.jcraft:jsch:0.1.42 - http://www.jcraft.com/jsch/)
     (Apache License 2.0) Compress-LZF (com.ning:compress-lzf:1.0.3 - http://github.com/ning/compress)
     (Apache 2.0) OkHttp (com.squareup.okhttp:okhttp:2.5.0 - https://github.com/square/okhttp/okhttp)
     (Apache 2.0) Okio (com.squareup.okio:okio:1.6.0 - https://github.com/square/okio/okio)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-client (com.sun.jersey:jersey-client:1.9 - https://jersey.java.net/jersey-client/)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-core (com.sun.jersey:jersey-core:1.9 - https://jersey.java.net/jersey-core/)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-grizzly2 (com.sun.jersey:jersey-grizzly2:1.9 - https://jersey.java.net/jersey-grizzly2/)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-json (com.sun.jersey:jersey-json:1.9 - https://jersey.java.net/jersey-json/)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-server (com.sun.jersey:jersey-server:1.9 - https://jersey.java.net/jersey-server/)
     (CDDL 1.1) (GPL2 w/ CPE) Jersey Apache HTTP Client 4.x (com.sun.jersey.contribs:jersey-apache-client4:1.9 - https://jersey.java.net/jersey-contribs/jersey-apache-client4/)
     (CDDL 1.1) (GPL2 w/ CPE) jersey-guice (com.sun.jersey.contribs:jersey-guice:1.9 - https://jersey.java.net/jersey-contribs/jersey-guice/)
     (CDDL 1.1) (GPL2 w/ CPE) Jersey Test Framework - Core (com.sun.jersey.jersey-test-framework:jersey-test-framework-core:1.9 - https://jersey.java.net/jersey-test-framework/jersey-test-framework-core/)
     (CDDL 1.1) (GPL2 w/ CPE) Jersey Test Framework - Grizzly 2 Module (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:1.9 - https://jersey.java.net/jersey-test-framework/jersey-test-framework-grizzly2/)
     (CDDL/GPLv2+CE) JavaMail API (com.sun.mail:javax.mail:1.5.0 - http://javamail.java.net/javax.mail)
     (CDDL 1.1) (GPL2 w/ CPE) JAXB RI (com.sun.xml.bind:jaxb-impl:2.2.3-1 - http://jaxb.java.net/)
     (BSD) ParaNamer Core (com.thoughtworks.paranamer:paranamer:2.3 - http://paranamer.codehaus.org/paranamer)
     (BSD) ParaNamer Core (com.thoughtworks.paranamer:paranamer:2.7 - http://paranamer.codehaus.org/paranamer)
     (Apache 2) chill-java (com.twitter:chill-java:0.5.0 - https://github.com/twitter/chill)
     (Apache 2) chill-java (com.twitter:chill-java:0.7.4 - https://github.com/twitter/chill)
     (Apache 2) chill (com.twitter:chill_2.10:0.5.0 - https://github.com/twitter/chill)
     (Apache 2) chill (com.twitter:chill_2.10:0.7.4 - https://github.com/twitter/chill)
     (Apache License, Version 2.0) config (com.typesafe:config:1.2.1 - https://github.com/typesafehub/config)
     (Apache License, Version 2.0) akka-actor (com.typesafe.akka:akka-actor_2.10:2.3.11 - http://akka.io/)
     (Apache License, Version 2.0) akka-actor (com.typesafe.akka:akka-actor_2.10:2.3.7 - http://akka.io/)
     (Apache License, Version 2.0) akka-remote (com.typesafe.akka:akka-remote_2.10:2.3.11 - http://akka.io/)
     (Apache License, Version 2.0) akka-remote (com.typesafe.akka:akka-remote_2.10:2.3.7 - http://akka.io/)
     (Apache License, Version 2.0) akka-slf4j (com.typesafe.akka:akka-slf4j_2.10:2.3.11 - http://akka.io/)
     (Apache License, Version 2.0) akka-slf4j (com.typesafe.akka:akka-slf4j_2.10:2.3.7 - http://akka.io/)
     (Apache License 2.0) Metrics Core Library (com.yammer.metrics:metrics-core:2.2.0 - http://metrics.codahale.com/metrics-core/)
     (Unknown license) commons-beanutils (commons-beanutils:commons-beanutils:1.7.0 - no url defined)
         Actual license: Apache 2.0.
     (The Apache Software License, Version 2.0) Commons BeanUtils (commons-beanutils:commons-beanutils:1.8.3 - http://commons.apache.org/beanutils/)
     (The Apache Software License, Version 2.0) Commons BeanUtils Bean Collections (commons-beanutils:commons-beanutils-bean-collections:1.8.3 - http://commons.apache.org/beanutils/)
     (The Apache Software License, Version 2.0) Commons BeanUtils Core (commons-beanutils:commons-beanutils-core:1.8.0 - http://commons.apache.org/beanutils/)
     (The Apache Software License, Version 2.0) Commons CLI (commons-cli:commons-cli:1.2 - http://commons.apache.org/cli/)
     (Apache License, Version 2.0) Apache Commons CLI (commons-cli:commons-cli:1.3.1 - http://commons.apache.org/proper/commons-cli/)
     (Apache License, Version 2.0) Apache Commons Codec (commons-codec:commons-codec:1.10 - http://commons.apache.org/proper/commons-codec/)
     (The Apache Software License, Version 2.0) Codec (commons-codec:commons-codec:1.3 - http://jakarta.apache.org/commons/codec/)
     (The Apache Software License, Version 2.0) Commons Codec (commons-codec:commons-codec:1.4 - http://commons.apache.org/codec/)
     (The Apache Software License, Version 2.0) Apache Commons Codec (commons-codec:commons-codec:1.9 - http://commons.apache.org/proper/commons-codec/)
     (The Apache Software License, Version 2.0) Commons Collections (commons-collections:commons-collections:3.2.1 - http://commons.apache.org/collections/)
     (The Apache Software License, Version 2.0) Commons Configuration (commons-configuration:commons-configuration:1.6 - http://commons.apache.org/${pom.artifactId.substring(8)}/)
     (The Apache Software License, Version 2.0) Commons Configuration (commons-configuration:commons-configuration:1.7 - http://commons.apache.org/configuration/)
     (The Apache Software License, Version 2.0) Commons Daemon (commons-daemon:commons-daemon:1.0.13 - http://commons.apache.org/daemon/)
     (The Apache Software License, Version 2.0) Digester (commons-digester:commons-digester:1.8 - http://jakarta.apache.org/commons/digester/)
     (The Apache Software License, Version 2.0) Commons Digester (commons-digester:commons-digester:1.8.1 - http://commons.apache.org/digester/)
     (The Apache Software License, Version 2.0) EL (commons-el:commons-el:1.0 - http://jakarta.apache.org/commons/el/)
     (Apache License) HttpClient (commons-httpclient:commons-httpclient:3.1 - http://jakarta.apache.org/httpcomponents/httpclient-3.x/)
     (The Apache Software License, Version 2.0) Commons IO (commons-io:commons-io:2.1 - http://commons.apache.org/io/)
     (The Apache Software License, Version 2.0) Commons IO (commons-io:commons-io:2.4 - http://commons.apache.org/io/)
     (The Apache Software License, Version 2.0) Commons Lang (commons-lang:commons-lang:2.4 - http://commons.apache.org/lang/)
     (The Apache Software License, Version 2.0) Commons Lang (commons-lang:commons-lang:2.5 - http://commons.apache.org/lang/)
     (The Apache Software License, Version 2.0) Commons Lang (commons-lang:commons-lang:2.6 - http://commons.apache.org/lang/)
     (The Apache Software License, Version 2.0) Commons Logging (commons-logging:commons-logging:1.1.1 - http://commons.apache.org/logging)
     (The Apache Software License, Version 2.0) Commons Logging (commons-logging:commons-logging:1.1.3 - http://commons.apache.org/proper/commons-logging/)
     (The Apache Software License, Version 2.0) Apache Commons Logging (commons-logging:commons-logging:1.2 - http://commons.apache.org/proper/commons-logging/)
     (The Apache Software License, Version 2.0) Commons Net (commons-net:commons-net:2.2 - http://commons.apache.org/net/)
     (The Apache Software License, Version 2.0) Commons Net (commons-net:commons-net:3.1 - http://commons.apache.org/net/)
     (The Apache Software License, Version 2.0) Commons Net (commons-net:commons-net:3.3 - http://commons.apache.org/proper/commons-net/)
     (The Apache Software License, Version 2.0) Flapdoodle Embedded MongoDB (de.flapdoodle.embed:de.flapdoodle.embed.mongo:1.50.1 - http://github.com/flapdoodle-oss/embedmongo.flapdoodle.de)
     (The Apache Software License, Version 2.0) Flapdoodle Embedded Process Util (de.flapdoodle.embed:de.flapdoodle.embed.process:1.50.1 - http://github.com/flapdoodle-oss/de.flapdoodle.embed.process)
     (The Apache Software License, Version 2.0) kryo serializers (de.javakaffee:kryo-serializers:0.39 - https://github.com/magro/kryo-serializers)
     (Apache License 2.0) Metrics Core (io.dropwizard.metrics:metrics-core:3.1.0 - http://metrics.codahale.com/metrics-core/)
     (Apache License 2.0) Metrics Core (io.dropwizard.metrics:metrics-core:3.1.2 - http://metrics.codahale.com/metrics-core/)
     (Apache License 2.0) Graphite Integration for Metrics (io.dropwizard.metrics:metrics-graphite:3.1.2 - http://metrics.codahale.com/metrics-graphite/)
     (Apache License 2.0) Jackson Integration for Metrics (io.dropwizard.metrics:metrics-json:3.1.0 - http://metrics.codahale.com/metrics-json/)
     (Apache License 2.0) Jackson Integration for Metrics (io.dropwizard.metrics:metrics-json:3.1.2 - http://metrics.codahale.com/metrics-json/)
     (Apache License 2.0) JVM Integration for Metrics (io.dropwizard.metrics:metrics-jvm:3.1.0 - http://metrics.codahale.com/metrics-jvm/)
     (Apache License 2.0) JVM Integration for Metrics (io.dropwizard.metrics:metrics-jvm:3.1.2 - http://metrics.codahale.com/metrics-jvm/)
     (BSD 3-Clause) io.grpc:grpc-all (io.grpc:grpc-all:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-auth (io.grpc:grpc-auth:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-context (io.grpc:grpc-context:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-core (io.grpc:grpc-core:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-netty (io.grpc:grpc-netty:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-okhttp (io.grpc:grpc-okhttp:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-protobuf (io.grpc:grpc-protobuf:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-protobuf-lite (io.grpc:grpc-protobuf-lite:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-protobuf-nano (io.grpc:grpc-protobuf-nano:1.0.1 - https://github.com/grpc/grpc-java)
     (BSD 3-Clause) io.grpc:grpc-stub (io.grpc:grpc-stub:1.0.1 - https://github.com/grpc/grpc-java)
     (Apache License, Version 2.0) The Netty Project (io.netty:netty:3.6.2.Final - http://netty.io/)
     (Apache License, Version 2.0) The Netty Project (io.netty:netty:3.7.0.Final - http://netty.io/)
     (Apache License, Version 2.0) The Netty Project (io.netty:netty:3.8.0.Final - http://netty.io/)
     (Apache License, Version 2.0) Netty/All-in-One (io.netty:netty-all:4.0.23.Final - http://netty.io/netty-all/)
     (Apache License, Version 2.0) Netty/All-in-One (io.netty:netty-all:4.0.27.Final - http://netty.io/netty-all/)
     (Apache License, Version 2.0) Netty/All-in-One (io.netty:netty-all:4.0.29.Final - http://netty.io/netty-all/)
     (Apache License, Version 2.0) Netty/Buffer (io.netty:netty-buffer:4.1.3.Final - http://netty.io/netty-buffer/)
     (Apache License, Version 2.0) Netty/Codec (io.netty:netty-codec:4.1.3.Final - http://netty.io/netty-codec/)
     (Apache License, Version 2.0) Netty/Codec/HTTP (io.netty:netty-codec-http:4.1.3.Final - http://netty.io/netty-codec-http/)
     (Apache License, Version 2.0) Netty/Codec/HTTP2 (io.netty:netty-codec-http2:4.1.3.Final - http://netty.io/netty-codec-http2/)
     (Apache License, Version 2.0) Netty/Common (io.netty:netty-common:4.1.3.Final - http://netty.io/netty-common/)
     (Apache License, Version 2.0) Netty/Handler (io.netty:netty-handler:4.1.3.Final - http://netty.io/netty-handler/)
     (Apache License, Version 2.0) Netty/Resolver (io.netty:netty-resolver:4.1.3.Final - http://netty.io/netty-resolver/)
     (Apache License, Version 2.0) Netty/TomcatNative [BoringSSL - Static] (io.netty:netty-tcnative-boringssl-static:1.1.33.Fork18 - https://github.com/netty/netty-tcnative/netty-tcnative-boringssl-static/)
     (Apache License, Version 2.0) Netty/Transport (io.netty:netty-transport:4.1.3.Final - http://netty.io/netty-transport/)
     (Apache License, Version 2.0) fastutil (it.unimi.dsi:fastutil:7.0.6 - http://fasutil.di.unimi.it/)
     (Common Development and Distribution License (CDDL) v1.0) JavaBeans Activation Framework (JAF) (javax.activation:activation:1.1 - http://java.sun.com/products/javabeans/jaf/index.jsp)
     (The Apache Software License, Version 2.0) javax.inject (javax.inject:javax.inject:1 - http://code.google.com/p/atinject/)
     (CDDL + GPLv2 with classpath exception) Java EE JMS API (javax.jms:jms-api:1.1-rev-1 - http://jcp.org/en/jsr/detail?id=914)
     (CDDL + GPLv2 with classpath exception) Java Servlet API (javax.servlet:javax.servlet-api:3.0.1 - http://servlet-spec.java.net)
     (Unknown license) servlet-api (javax.servlet:servlet-api:2.5 - no url defined)
         Comment: CDDL choice.
     (Unknown license) jsp-api (javax.servlet.jsp:jsp-api:2.1 - no url defined)
         Comment: CDDL choice.
     (The Apache Software License, Version 2.0) Bean Validation API (javax.validation:validation-api:1.1.0.Final - http://beanvalidation.org)
     (CDDL 1.1) (GPL2 w/ CPE) JAXB API bundle for GlassFish V3 (javax.xml.bind:jaxb-api:2.2.2 - https://jaxb.dev.java.net/)
     (COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0) (GNU General Public Library) Streaming API for XML (javax.xml.stream:stax-api:1.0-2 - no url defined)
     (BSD) JLine (jline:jline:0.9.94 - http://jline.sourceforge.net)
     (The BSD License) JLine (jline:jline:2.11 - http://nexus.sonatype.org/oss-repository-hosting.html/jline)
     (Apache 2) Joda-Time (joda-time:joda-time:2.4 - http://www.joda.org/joda-time/)
     (Common Public License Version 1.0) JUnit (junit:junit:4.11 - http://junit.org)
     (The Apache Software License, Version 2.0) Apache Log4j (log4j:log4j:1.2.17 - http://logging.apache.org/log4j/1.2/)
     (The Apache Software License, Version 2.0) Byte Buddy (without dependencies) (net.bytebuddy:byte-buddy:1.4.3 - http://bytebuddy.net/byte-buddy)
     (MIT license) mbassador (net.engio:mbassador:1.1.9 - https://github.com/bennidi/mbassador)
     (Apache License, Version 2.0) An open source Java toolkit for Amazon S3 (net.java.dev.jets3t:jets3t:0.6.1 - http://jets3t.s3.amazonaws.com/index.html)
     (Apache License, Version 2.0) An open source Java toolkit for Amazon S3 (net.java.dev.jets3t:jets3t:0.7.1 - http://jets3t.s3.amazonaws.com/index.html)
     (Apache License, Version 2.0) An open source Java toolkit for Amazon S3 (net.java.dev.jets3t:jets3t:0.9.0 - http://www.jets3t.org)
     (ASL, version 2) (LGPL, version 2.1) Java Native Access (net.java.dev.jna:jna:4.0.0 - https://github.com/twall/jna)
     (ASL, version 2) (LGPL, version 2.1) Java Native Access Platform (net.java.dev.jna:jna-platform:4.0.0 - https://github.com/twall/jna)
     (The Apache Software License, Version 2.0) LZ4 and xxHash (net.jpountz.lz4:lz4:1.2.0 - https://github.com/jpountz/lz4-java)
     (The Apache Software License, Version 2.0) LZ4 and xxHash (net.jpountz.lz4:lz4:1.3.0 - https://github.com/jpountz/lz4-java)
     (The Apache Software License, Version 2.0) zip4j (net.lingala.zip4j:zip4j:1.3.2 - http://www.lingala.net/zip4j/)
     (MIT License) pyrolite (net.razorvine:pyrolite:4.9 - https://github.com/irmen/Pyrolite)
     (The MIT License) JOpt Simple (net.sf.jopt-simple:jopt-simple:3.2 - http://jopt-simple.sourceforge.net)
     (The New BSD License) Py4J (net.sf.py4j:py4j:0.9 - http://www.py4j.org/)
     (Apache License, Version 2.0) ActiveMQ :: Broker (org.apache.activemq:activemq-broker:5.13.1 - http://activemq.apache.org/activemq-broker)
     (Apache License, Version 2.0) ActiveMQ :: Client (org.apache.activemq:activemq-client:5.13.1 - http://activemq.apache.org/activemq-client)
     (The Apache Software License, Version 2.0) ActiveMQ :: Client (org.apache.activemq:activemq-client:5.8.0 - http://activemq.apache.org/activemq-client)
     (Apache License, Version 2.0) ActiveMQ :: KahaDB Store (org.apache.activemq:activemq-kahadb-store:5.13.1 - http://activemq.apache.org/activemq-kahadb-store)
     (Apache License, Version 2.0) ActiveMQ :: Openwire Legacy Support (org.apache.activemq:activemq-openwire-legacy:5.13.1 - http://activemq.apache.org/activemq-openwire-legacy)
     (The Apache Software License, Version 2.0) ActiveMQ Protocol Buffers Implementation and Compiler (org.apache.activemq.protobuf:activemq-protobuf:1.1 - http://activemq.apache.org/activemq-protobuf)
     (The Apache Software License, Version 2.0) Apache Ant Core (org.apache.ant:ant:1.9.2 - http://ant.apache.org/)
     (The Apache Software License, Version 2.0) Apache Ant Launcher (org.apache.ant:ant-launcher:1.9.2 - http://ant.apache.org/)
     (The Apache Software License, Version 2.0) Apache Apex API (org.apache.apex:apex-api:3.5.0-SNAPSHOT - http://apex.apache.org/apex-api)
     (The Apache Software License, Version 2.0) Apache Apex Buffer Server (org.apache.apex:apex-bufferserver:3.5.0-SNAPSHOT - http://apex.apache.org/apex-bufferserver)
     (The Apache Software License, Version 2.0) Apache Apex Common Library (org.apache.apex:apex-common:3.5.0-SNAPSHOT - http://apex.apache.org/apex-common)
     (The Apache Software License, Version 2.0) Apache Apex Stream Processing Engine (org.apache.apex:apex-engine:3.5.0-SNAPSHOT - http://apex.apache.org/apex-engine)
     (The Apache Software License, Version 2.0) Apache Apex Shaded ning async-http-client library (org.apache.apex:apex-shaded-ning19:1.0.0 - no url defined)
     (The Apache Software License, Version 2.0) Apache Apex Malhar Library (org.apache.apex:malhar-library:3.4.0 - http://apex.apache.org/malhar-library)
     (The Apache Software License, Version 2.0) Apache Avro (org.apache.avro:avro:1.8.1 - http://avro.apache.org)
     (The Apache Software License, Version 2.0) Apache Avro IPC (org.apache.avro:avro-ipc:1.8.1 - http://avro.apache.org)
     (The Apache Software License, Version 2.0) Apache Avro Mapred API (org.apache.avro:avro-mapred:1.8.1 - http://avro.apache.org/avro-mapred)
     (Apache License, Version 2.0) Apache Beam :: Examples :: Java (org.apache.beam:beam-examples-java:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-examples-parent/beam-examples-java)
     (Apache License, Version 2.0) Apache Beam :: Runners :: Core Java (org.apache.beam:beam-runners-core-java:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-runners-parent/beam-runners-core-java)
     (Apache License, Version 2.0) Apache Beam :: Runners :: Direct Java (org.apache.beam:beam-runners-direct-java:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-runners-parent/beam-runners-direct-java)
     (Apache License, Version 2.0) Apache Beam :: Runners :: Flink :: Core (org.apache.beam:beam-runners-flink_2.10:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-runners-parent/beam-runners-flink-parent/beam-runners-flink_2.10)
     (Apache License, Version 2.0) Apache Beam :: Runners :: Google Cloud Dataflow (org.apache.beam:beam-runners-google-cloud-dataflow-java:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-runners-parent/beam-runners-google-cloud-dataflow-java)
     (Apache License, Version 2.0) Apache Beam :: Runners :: Spark (org.apache.beam:beam-runners-spark:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-runners-parent/beam-runners-spark)
     (Apache License, Version 2.0) Apache Beam :: SDKs :: Java :: Core (org.apache.beam:beam-sdks-java-core:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-sdks-parent/beam-sdks-java-parent/beam-sdks-java-core)
     (Apache License, Version 2.0) Apache Beam :: SDKs :: Java :: IO :: Google Cloud Platform (org.apache.beam:beam-sdks-java-io-google-cloud-platform:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-sdks-parent/beam-sdks-java-parent/beam-sdks-java-io-parent/beam-sdks-java-io-google-cloud-platform)
     (Apache License, Version 2.0) Apache Beam :: SDKs :: Java :: IO :: Kafka (org.apache.beam:beam-sdks-java-io-kafka:0.4.0-incubating-SNAPSHOT - http://beam.incubator.apache.org/beam-sdks-parent/beam-sdks-java-parent/beam-sdks-java-io-parent/beam-sdks-java-io-kafka)
     (The Apache Software License, Version 2.0) Apache BVal :: bval-core (org.apache.bval:bval-core:0.5 - http://bval.apache.org/bval-core/)
     (The Apache Software License, Version 2.0) Apache BVal :: bval-jsr303 (org.apache.bval:bval-jsr303:0.5 - http://bval.apache.org/bval-jsr303/)
     (The Apache Software License, Version 2.0) Apache Commons Compress (org.apache.commons:commons-compress:1.9 - http://commons.apache.org/proper/commons-compress/)
     (The Apache Software License, Version 2.0) Apache Commons DBCP (org.apache.commons:commons-dbcp2:2.1.1 - http://commons.apache.org/dbcp/)
     (The Apache Software License, Version 2.0) Commons Lang (org.apache.commons:commons-lang3:3.1 - http://commons.apache.org/lang/)
     (The Apache Software License, Version 2.0) Apache Commons Lang (org.apache.commons:commons-lang3:3.3.2 - http://commons.apache.org/proper/commons-lang/)
     (The Apache Software License, Version 2.0) Commons Math (org.apache.commons:commons-math:2.1 - http://commons.apache.org/math/)
     (The Apache Software License, Version 2.0) Commons Math (org.apache.commons:commons-math:2.2 - http://commons.apache.org/math/)
     (The Apache Software License, Version 2.0) Commons Math (org.apache.commons:commons-math3:3.1.1 - http://commons.apache.org/math/)
     (The Apache Software License, Version 2.0) Apache Commons Math (org.apache.commons:commons-math3:3.4.1 - http://commons.apache.org/proper/commons-math/)
     (The Apache Software License, Version 2.0) Apache Commons Math (org.apache.commons:commons-math3:3.5 - http://commons.apache.org/proper/commons-math/)
     (The Apache Software License, Version 2.0) Apache Commons Pool (org.apache.commons:commons-pool2:2.4.2 - http://commons.apache.org/proper/commons-pool/)
     (The Apache Software License, Version 2.0) Curator Client (org.apache.curator:curator-client:2.4.0 - http://curator.apache.org/curator-client)
     (The Apache Software License, Version 2.0) Curator Client (org.apache.curator:curator-client:2.7.1 - http://curator.apache.org/curator-client)
     (The Apache Software License, Version 2.0) Curator Framework (org.apache.curator:curator-framework:2.4.0 - http://curator.apache.org/curator-framework)
     (The Apache Software License, Version 2.0) Curator Framework (org.apache.curator:curator-framework:2.7.1 - http://curator.apache.org/curator-framework)
     (The Apache Software License, Version 2.0) Curator Recipes (org.apache.curator:curator-recipes:2.4.0 - http://curator.apache.org/curator-recipes)
     (The Apache Software License, Version 2.0) Curator Recipes (org.apache.curator:curator-recipes:2.7.1 - http://curator.apache.org/curator-recipes)
     (The Apache Software License, Version 2.0) Curator Testing (org.apache.curator:curator-test:2.8.0 - http://curator.apache.org/curator-test)
     (Apache 2) Apache Derby Database Engine and Embedded JDBC Driver (org.apache.derby:derby:10.12.1.1 - http://db.apache.org/derby/derby/)
     (Apache 2) Apache Derby Client JDBC Driver (org.apache.derby:derbyclient:10.12.1.1 - http://db.apache.org/derby/derbyclient/)
     (Apache 2) Apache Derby Network Server (org.apache.derby:derbynet:10.12.1.1 - http://db.apache.org/derby/derbynet/)
     (The Apache Software License, Version 2.0) Apache Directory API ASN.1 API (org.apache.directory.api:api-asn1-api:1.0.0-M20 - http://directory.apache.org/api-parent/api-asn1-parent/api-asn1-api/)
     (The Apache Software License, Version 2.0) Apache Directory LDAP API Utilities (org.apache.directory.api:api-util:1.0.0-M20 - http://directory.apache.org/api-parent/api-util/)
     (The Apache Software License, Version 2.0) ApacheDS I18n (org.apache.directory.server:apacheds-i18n:2.0.0-M15 - http://directory.apache.org/apacheds/1.5/apacheds-i18n)
     (The Apache Software License, Version 2.0) ApacheDS Protocol Kerberos Codec (org.apache.directory.server:apacheds-kerberos-codec:2.0.0-M15 - http://directory.apache.org/apacheds/1.5/apacheds-kerberos-codec)
     (The Apache Software License, Version 2.0) flink-annotations (org.apache.flink:flink-annotations:1.1.2 - http://flink.apache.org/flink-annotations)
     (The Apache Software License, Version 2.0) flink-clients (org.apache.flink:flink-clients_2.10:1.1.2 - http://flink.apache.org/flink-clients_2.10)
     (The Apache Software License, Version 2.0) flink-connector-kafka-0.8 (org.apache.flink:flink-connector-kafka-0.8_2.10:1.1.2 - http://flink.apache.org/flink-streaming-connectors/flink-connector-kafka-0.8_2.10)
     (The Apache Software License, Version 2.0) flink-connector-kafka-base (org.apache.flink:flink-connector-kafka-base_2.10:1.1.2 - http://flink.apache.org/flink-streaming-connectors/flink-connector-kafka-base_2.10)
     (The Apache Software License, Version 2.0) flink-core (org.apache.flink:flink-core:1.1.2 - http://flink.apache.org/flink-core)
     (The Apache Software License, Version 2.0) flink-java (org.apache.flink:flink-java:1.1.2 - http://flink.apache.org/flink-java)
     (The Apache Software License, Version 2.0) flink-metrics-core (org.apache.flink:flink-metrics-core:1.1.2 - http://flink.apache.org/flink-metrics/flink-metrics-core)
     (The Apache Software License, Version 2.0) flink-optimizer (org.apache.flink:flink-optimizer_2.10:1.1.2 - http://flink.apache.org/flink-optimizer_2.10)
     (The Apache Software License, Version 2.0) flink-runtime (org.apache.flink:flink-runtime_2.10:1.1.2 - http://flink.apache.org/flink-runtime_2.10)
     (The Apache Software License, Version 2.0) flink-shaded-hadoop2 (org.apache.flink:flink-shaded-hadoop2:1.1.2 - http://flink.apache.org/flink-shaded-hadoop/flink-shaded-hadoop2)
     (The Apache Software License, Version 2.0) flink-streaming-java (org.apache.flink:flink-streaming-java_2.10:1.1.2 - http://flink.apache.org/flink-streaming-java_2.10)
     (The Apache Software License, Version 2.0) flink-test-utils-junit (org.apache.flink:flink-test-utils-junit:1.1.2 - http://flink.apache.org/flink-test-utils-parent/flink-test-utils-junit)
     (The Apache Software License, Version 2.0) flink-test-utils (org.apache.flink:flink-test-utils_2.10:1.1.2 - http://flink.apache.org/flink-test-utils-parent/flink-test-utils_2.10)
     (The Apache Software License, Version 2.0) force-shading (org.apache.flink:force-shading:1.1.2 - http://www.apache.org/force-shading/)
     (The Apache Software License, Version 2.0) J2EE Management 1.1 (org.apache.geronimo.specs:geronimo-j2ee-management_1.1_spec:1.0.1 - http://geronimo.apache.org/specs/geronimo-j2ee-management_1.1_spec)
     (The Apache Software License, Version 2.0) JMS 1.1 (org.apache.geronimo.specs:geronimo-jms_1.1_spec:1.1.1 - http://geronimo.apache.org/specs/geronimo-jms_1.1_spec)
     (The Apache Software License, Version 2.0) Apache Hadoop Annotations (org.apache.hadoop:hadoop-annotations:2.2.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Annotations (org.apache.hadoop:hadoop-annotations:2.7.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Annotations (org.apache.hadoop:hadoop-annotations:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) Apache Hadoop Auth (org.apache.hadoop:hadoop-auth:2.2.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Auth (org.apache.hadoop:hadoop-auth:2.7.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Auth (org.apache.hadoop:hadoop-auth:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) Apache Hadoop Client (org.apache.hadoop:hadoop-client:2.2.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Client (org.apache.hadoop:hadoop-client:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) Apache Hadoop Common (org.apache.hadoop:hadoop-common:2.2.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Common (org.apache.hadoop:hadoop-common:2.7.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop Common (org.apache.hadoop:hadoop-common:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) Apache Hadoop HDFS (org.apache.hadoop:hadoop-hdfs:2.2.0 - no url defined)
     (Apache License, Version 2.0) Apache Hadoop HDFS (org.apache.hadoop:hadoop-hdfs:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-mapreduce-client-app (org.apache.hadoop:hadoop-mapreduce-client-app:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-app (org.apache.hadoop:hadoop-mapreduce-client-app:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-mapreduce-client-common (org.apache.hadoop:hadoop-mapreduce-client-common:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-common (org.apache.hadoop:hadoop-mapreduce-client-common:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-mapreduce-client-core (org.apache.hadoop:hadoop-mapreduce-client-core:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-core (org.apache.hadoop:hadoop-mapreduce-client-core:2.7.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-core (org.apache.hadoop:hadoop-mapreduce-client-core:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-mapreduce-client-jobclient (org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-jobclient (org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-mapreduce-client-shuffle (org.apache.hadoop:hadoop-mapreduce-client-shuffle:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-mapreduce-client-shuffle (org.apache.hadoop:hadoop-mapreduce-client-shuffle:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-yarn-api (org.apache.hadoop:hadoop-yarn-api:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-api (org.apache.hadoop:hadoop-yarn-api:2.7.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-api (org.apache.hadoop:hadoop-yarn-api:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-yarn-client (org.apache.hadoop:hadoop-yarn-client:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-client (org.apache.hadoop:hadoop-yarn-client:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-yarn-common (org.apache.hadoop:hadoop-yarn-common:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-common (org.apache.hadoop:hadoop-yarn-common:2.7.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-common (org.apache.hadoop:hadoop-yarn-common:2.7.1 - no url defined)
     (The Apache Software License, Version 2.0) hadoop-yarn-server-common (org.apache.hadoop:hadoop-yarn-server-common:2.2.0 - no url defined)
     (Apache License, Version 2.0) hadoop-yarn-server-common (org.apache.hadoop:hadoop-yarn-server-common:2.7.0 - no url defined)
     (The Apache Software License, Version 2.0) htrace-core (org.apache.htrace:htrace-core:3.1.0-incubating - http://incubator.apache.org/projects/htrace.html)
     (Apache License) HttpClient (org.apache.httpcomponents:httpclient:4.0.1 - http://hc.apache.org/httpcomponents-client)
     (Apache License, Version 2.0) Apache HttpClient (org.apache.httpcomponents:httpclient:4.3.5 - http://hc.apache.org/httpcomponents-client)
     (Apache License, Version 2.0) Apache HttpClient (org.apache.httpcomponents:httpclient:4.5.2 - http://hc.apache.org/httpcomponents-client)
     (Apache License) HttpCore (org.apache.httpcomponents:httpcore:4.0.1 - http://hc.apache.org/httpcomponents-core/)
     (Apache License) HttpCore (org.apache.httpcomponents:httpcore:4.1.2 - http://hc.apache.org/httpcomponents-core-ga)
     (Apache License, Version 2.0) Apache HttpCore (org.apache.httpcomponents:httpcore:4.3.2 - http://hc.apache.org/httpcomponents-core-ga)
     (Apache License, Version 2.0) Apache HttpCore (org.apache.httpcomponents:httpcore:4.4.4 - http://hc.apache.org/httpcomponents-core-ga)
     (The Apache Software License, Version 2.0) Apache Ivy (org.apache.ivy:ivy:2.4.0 - http://ant.apache.org/ivy/)
     (The Apache Software License, Version 2.0) Apache Kafka (org.apache.kafka:kafka-clients:0.8.2.2 - http://kafka.apache.org)
     (The Apache Software License, Version 2.0) Apache Kafka (org.apache.kafka:kafka-clients:0.9.0.1 - http://kafka.apache.org)
     (The Apache Software License, Version 2.0) Apache Kafka (org.apache.kafka:kafka_2.10:0.8.2.2 - http://kafka.apache.org)
     (The Apache Software License, Version 2.0) Apache Kafka (org.apache.kafka:kafka_2.10:0.9.0.1 - http://kafka.apache.org)
     (The Apache Software License, Version 2.0) mesos (org.apache.mesos:mesos:0.21.1 - http://mesos.apache.org)
     (The Apache Software License, Version 2.0) Apache Sling JSON Library (org.apache.sling:org.apache.sling.commons.json:2.0.6 - http://sling.apache.org/org.apache.sling.commons.json)
     (Apache 2.0 License) Spark Project Core (org.apache.spark:spark-core_2.10:1.6.2 - http://spark.apache.org/)
     (Apache 2.0 License) Spark Project Launcher (org.apache.spark:spark-launcher_2.10:1.6.2 - http://spark.apache.org/)
     (Apache 2.0 License) Spark Project Networking (org.apache.spark:spark-network-common_2.10:1.6.2 - http://spark.apache.org/)
     (Apache 2.0 License) Spark Project Shuffle Streaming Service (org.apache.spark:spark-network-shuffle_2.10:1.6.2 - http://spark.apache.org/)
     (Apache 2.0 License) Spark Project Streaming (org.apache.spark:spark-streaming_2.10:1.6.2 - http://spark.apache.org/)
     (Apache 2.0 License) Spark Project Unsafe (org.apache.spark:spark-unsafe_2.10:1.6.2 - http://spark.apache.org/)
     (The Apache Software License, Version 2.0) Apache Velocity (org.apache.velocity:velocity:1.7 - http://velocity.apache.org/engine/devel/)
     (http://asm.ow2.org/license.html) (http://www.apache.org/licenses/LICENSE-2.0.txt) Apache XBean :: ASM 5 shaded (repackaged) (org.apache.xbean:xbean-asm5-shaded:4.3 - http://geronimo.apache.org/maven/xbean/4.3/xbean-asm5-shaded)
     (http://asm.ow2.org/license.html) (http://www.apache.org/licenses/LICENSE-2.0.txt) Apache XBean :: ASM 5 shaded (repackaged) (org.apache.xbean:xbean-asm5-shaded:4.4 - http://geronimo.apache.org/maven/xbean/4.4/xbean-asm5-shaded)
     (Unknown license) zookeeper (org.apache.zookeeper:zookeeper:3.4.5 - no url defined)
         Actual license: Apache 2.0.
     (Unknown license) zookeeper (org.apache.zookeeper:zookeeper:3.4.6 - no url defined)
         Actual license: Apache 2.0.
     (Apache License, Version 2.0) AssertJ fluent assertions (org.assertj:assertj-core:2.5.0 - http://assertj.org/assertj-core)
     (BSD) grizzled-slf4j (org.clapper:grizzled-slf4j_2.10:1.0.2 - https://github.com/bmc/grizzled-slf4j/#readme)
     (The Apache Software License, Version 2.0) Jackson (org.codehaus.jackson:jackson-core-asl:1.8.8 - http://jackson.codehaus.org)
     (The Apache Software License, Version 2.0) Jackson (org.codehaus.jackson:jackson-core-asl:1.9.13 - http://jackson.codehaus.org)
     (GNU Lesser General Public License (LGPL), Version 2.1) (The Apache Software License, Version 2.0) JAX-RS provider for JSON content type (org.codehaus.jackson:jackson-jaxrs:1.8.3 - http://jackson.codehaus.org)
     (GNU Lesser General Public License (LGPL), Version 2.1) (The Apache Software License, Version 2.0) JAX-RS provider for JSON content type (org.codehaus.jackson:jackson-jaxrs:1.9.13 - http://jackson.codehaus.org)
     (The Apache Software License, Version 2.0) Data Mapper for Jackson (org.codehaus.jackson:jackson-mapper-asl:1.8.8 - http://jackson.codehaus.org)
     (The Apache Software License, Version 2.0) Data Mapper for Jackson (org.codehaus.jackson:jackson-mapper-asl:1.9.13 - http://jackson.codehaus.org)
     (GNU Lesser General Public License (LGPL), Version 2.1) (The Apache Software License, Version 2.0) Xml Compatibility extensions for Jackson (org.codehaus.jackson:jackson-xc:1.8.3 - http://jackson.codehaus.org)
     (GNU Lesser General Public License (LGPL), Version 2.1) (The Apache Software License, Version 2.0) Xml Compatibility extensions for Jackson (org.codehaus.jackson:jackson-xc:1.9.13 - http://jackson.codehaus.org)
     (New BSD License) Commons Compiler (org.codehaus.janino:commons-compiler:2.7.8 - http://docs.codehaus.org/display/JANINO/Home/commons-compiler)
     (Unknown license) Jettison (org.codehaus.jettison:jettison:1.1 - no url defined)
         Actual licence: Apache 2.0.
     (The BSD License) Stax2 API (org.codehaus.woodstox:stax2-api:3.1.4 - http://wiki.fasterxml.com/WoodstoxStax2)
     (The Apache Software License, Version 2.0) Woodstox (org.codehaus.woodstox:woodstox-core-asl:4.4.1 - http://woodstox.codehaus.org)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Continuation (org.eclipse.jetty:jetty-continuation:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-continuation)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Http Utility (org.eclipse.jetty:jetty-http:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-http)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: IO Utility (org.eclipse.jetty:jetty-io:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-io)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Security (org.eclipse.jetty:jetty-security:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-security)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Server Core (org.eclipse.jetty:jetty-server:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-server)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Servlet Handling (org.eclipse.jetty:jetty-servlet:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-servlet)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Utilities (org.eclipse.jetty:jetty-util:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-util)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty :: Websocket (org.eclipse.jetty:jetty-websocket:8.1.10.v20130312 - http://www.eclipse.org/jetty/jetty-websocket)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty Orbit :: Servlet API (org.eclipse.jetty.orbit:javax.servlet:3.0.0.v201112011016 - http://www.eclipse.org/jetty/jetty-orbit/javax.servlet)
     (The Apache Software License, Version 2.0) hawtbuf (org.fusesource.hawtbuf:hawtbuf:1.11 - http://hawtbuf.fusesource.org/hawtbuf)
     (The Apache Software License, Version 2.0) hawtbuf (org.fusesource.hawtbuf:hawtbuf:1.9 - http://hawtbuf.fusesource.org/hawtbuf)
     (The BSD 3-Clause License) leveldbjni-all (org.fusesource.leveldbjni:leveldbjni-all:1.8 - http://leveldbjni.fusesource.org/leveldbjni-all)
     (CDDL + GPLv2 with classpath exception) javax.servlet API v.3.0 (org.glassfish:javax.servlet:3.1 - http://jcp.org/en/jsr/detail?id=315)
     (CDDL+GPL) management-api (org.glassfish.external:management-api:3.0.0-b012 - http://kenai.com/hg/gmbal~gf_common)
     (CDDL+GPL) gmbal-api-only (org.glassfish.gmbal:gmbal-api-only:3.0.0-b023 - http://kenai.com/hg/gmbal~master)
     (CDDL+GPL) grizzly-framework (org.glassfish.grizzly:grizzly-framework:2.1.2 - http://grizzly.java.net/grizzly-framework)
     (CDDL+GPL) grizzly-http (org.glassfish.grizzly:grizzly-http:2.1.2 - http://grizzly.java.net/grizzly-http)
     (CDDL+GPL) grizzly-http-server (org.glassfish.grizzly:grizzly-http-server:2.1.2 - http://grizzly.java.net/grizzly-http-server)
     (CDDL+GPL) grizzly-http-servlet (org.glassfish.grizzly:grizzly-http-servlet:2.1.2 - http://grizzly.java.net/grizzly-http-servlet)
     (CDDL+GPL) grizzly-rcm (org.glassfish.grizzly:grizzly-rcm:2.1.2 - http://grizzly.java.net/grizzly-rcm)
     (New BSD License) Hamcrest All (org.hamcrest:hamcrest-all:1.3 - https://github.com/hamcrest/JavaHamcrest/hamcrest-all)
     (New BSD License) Hamcrest Core (org.hamcrest:hamcrest-core:1.3 - https://github.com/hamcrest/JavaHamcrest/hamcrest-core)
     (Apache License 2.0) (LGPL 2.1) (MPL 1.1) Javassist (org.javassist:javassist:3.18.2-GA - http://www.javassist.org/)
     (Apache License, Version 2.0) Java Concurrency Tools Core Library (org.jctools:jctools-core:1.1 - https://github.com/JCTools)
     (ASL) json4s-ast (org.json4s:json4s-ast_2.10:3.2.10 - https://github.com/json4s/json4s)
     (ASL) json4s-core (org.json4s:json4s-core_2.10:3.2.10 - https://github.com/json4s/json4s)
     (ASL) json4s-jackson (org.json4s:json4s-jackson_2.10:3.2.10 - https://github.com/json4s/json4s)
     (The MIT License) Mockito (org.mockito:mockito-all:1.9.5 - http://www.mockito.org)
     (The Apache Software License, Version 2.0) MongoDB Java Driver (org.mongodb:mongo-java-driver:3.2.2 - http://www.mongodb.org)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty Server (org.mortbay.jetty:jetty:6.1.26 - http://www.eclipse.org/jetty/jetty-parent/project/modules/jetty)
     (Apache Software License - Version 2.0) (Eclipse Public License - Version 1.0) Jetty Utilities (org.mortbay.jetty:jetty-util:6.1.26 - http://www.eclipse.org/jetty/jetty-parent/project/jetty-util)
     (Apache 2) Objenesis (org.objenesis:objenesis:1.2 - http://objenesis.googlecode.com/svn/docs/index.html)
     (Apache 2) Objenesis (org.objenesis:objenesis:2.1 - http://objenesis.org)
     (BSD) ASM Core (org.ow2.asm:asm:4.0 - http://asm.objectweb.org/asm/)
     (Apache 2) RoaringBitmap (org.roaringbitmap:RoaringBitmap:0.5.11 - https://github.com/lemire/RoaringBitmap)
     (BSD-like) Scala Compiler (org.scala-lang:scala-compiler:2.10.0 - http://www.scala-lang.org/)
     (BSD-like) Scala Library (org.scala-lang:scala-library:2.10.4 - http://www.scala-lang.org/)
     (BSD-like) Scala Library (org.scala-lang:scala-library:2.10.5 - http://www.scala-lang.org/)
     (BSD-like) Scala Compiler (org.scala-lang:scala-reflect:2.10.6 - http://www.scala-lang.org/)
     (BSD-like) Scalap (org.scala-lang:scalap:2.10.0 - http://www.scala-lang.org/)
     (MIT License) JCL 1.1.1 implemented over SLF4J (org.slf4j:jcl-over-slf4j:1.7.10 - http://www.slf4j.org)
     (MIT License) JUL to SLF4J bridge (org.slf4j:jul-to-slf4j:1.7.10 - http://www.slf4j.org)
     (MIT License) SLF4J API Module (org.slf4j:slf4j-api:1.7.14 - http://www.slf4j.org)
     (MIT License) SLF4J JDK14 Binding (org.slf4j:slf4j-jdk14:1.7.14 - http://www.slf4j.org)
     (MIT License) SLF4J LOG4J-12 Binding (org.slf4j:slf4j-log4j12:1.7.10 - http://www.slf4j.org)
     (MIT License) SLF4J LOG4J-12 Binding (org.slf4j:slf4j-log4j12:1.7.5 - http://www.slf4j.org)
     (MIT License) SLF4J LOG4J-12 Binding (org.slf4j:slf4j-log4j12:1.7.7 - http://www.slf4j.org)
     (The Apache License, Version 2.0) empty (org.spark-project.spark:unused:1.0.0 - http://nexus.sonatype.org/oss-repository-hosting.html/unused)
     (Apache License) Tachyon Clients - Distribution (org.tachyonproject:tachyon-client:0.8.2 - http://tachyon-project.org/tachyon-clients/tachyon-client/)
     (Apache License) Tachyon Under File System - HDFS (org.tachyonproject:tachyon-underfs-hdfs:0.8.2 - http://tachyon-project.org/tachyon-underfs/tachyon-underfs-hdfs/)
     (Apache License) Tachyon Under File System - Local FS (org.tachyonproject:tachyon-underfs-local:0.8.2 - http://tachyon-project.org/tachyon-underfs/tachyon-underfs-local/)
     (Apache License) Tachyon Under File System - S3 (org.tachyonproject:tachyon-underfs-s3:0.8.2 - http://tachyon-project.org/tachyon-underfs/tachyon-underfs-s3/)
     (Public Domain) XZ for Java (org.tukaani:xz:1.0 - http://tukaani.org/xz/java.html)
     (Public Domain) XZ for Java (org.tukaani:xz:1.5 - http://tukaani.org/xz/java.html)
     (Apache License, Version 2.0) Uncommons Maths (org.uncommons.maths:uncommons-maths:1.2.2a - http://maths.uncommons.org/)
     (The Apache Software License, Version 2.0) snappy-java (org.xerial.snappy:snappy-java:1.1.2.1 - https://github.com/xerial/snappy-java)
     (Unknown license) oro (oro:oro:2.0.8 - no url defined)
         Actual license: Apache 2.0.
     (The Apache Software License, Version 2.0) StAX API (stax:stax-api:1.0.1 - http://stax.codehaus.org/)
     (The Apache Software License, Version 2.0) jasper-compiler (tomcat:jasper-compiler:5.5.23 - http://tomcat.apache.org/jasper-compiler)
     (The Apache Software License, Version 2.0) jasper-runtime (tomcat:jasper-runtime:5.5.23 - http://tomcat.apache.org/jasper-runtime)
     (The Apache Software License, Version 2.0) Xerces2 Java Parser (xerces:xercesImpl:2.9.1 - http://xerces.apache.org/xerces2-j)
     (The Apache Software License, Version 2.0) XML Commons External Components XML APIs (xml-apis:xml-apis:1.3.04 - http://xml.apache.org/commons/components/external/)
     (The BSD License) xmlenc Library (xmlenc:xmlenc:0.52 - http://xmlenc.sourceforge.net)
```

### Dependency tree
The following is a verbose dependency tree of the project. This complements the previous list to assess where the dependency is coming from, whether it is distributed or not, and whether it is optional.
```
[INFO] Scanning for projects...
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Build Order:
[INFO]
[INFO] Apache Beam :: Parent
[INFO] Apache Beam :: SDKs :: Java :: Build Tools
[INFO] Apache Beam :: SDKs
[INFO] Apache Beam :: SDKs :: Java
[INFO] Apache Beam :: SDKs :: Java :: Core
[INFO] Apache Beam :: Runners
[INFO] Apache Beam :: Runners :: Core Java
[INFO] Apache Beam :: Runners :: Direct Java
[INFO] Apache Beam :: Runners :: Google Cloud Dataflow
[INFO] Apache Beam :: SDKs :: Java :: IO
[INFO] Apache Beam :: SDKs :: Java :: IO :: Google Cloud Platform
[INFO] Apache Beam :: SDKs :: Java :: IO :: HDFS
[INFO] Apache Beam :: SDKs :: Java :: IO :: JMS
[INFO] Apache Beam :: SDKs :: Java :: IO :: Kafka
[INFO] Apache Beam :: SDKs :: Java :: IO :: Kinesis
[INFO] Apache Beam :: SDKs :: Java :: IO :: MongoDB
[INFO] Apache Beam :: SDKs :: Java :: IO :: JDBC
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes :: Starter
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes :: Examples
[INFO] Apache Beam :: SDKs :: Java :: Extensions
[INFO] Apache Beam :: SDKs :: Java :: Extensions :: Join library
[INFO] Apache Beam :: SDKs :: Java :: Extensions :: Sorter
[INFO] Apache Beam :: SDKs :: Java :: Java 8 Tests
[INFO] Apache Beam :: Runners :: Flink
[INFO] Apache Beam :: Runners :: Flink :: Core
[INFO] Apache Beam :: Runners :: Flink :: Examples
[INFO] Apache Beam :: Runners :: Spark
[INFO] Apache Beam :: Runners :: Apex
[INFO] Apache Beam :: Examples
[INFO] Apache Beam :: Examples :: Java
[INFO] Apache Beam :: Examples :: Java 8
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Parent 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-parent ---
[INFO] org.apache.beam:beam-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Build Tools 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-build-tools ---
[INFO] org.apache.beam:beam-sdks-java-build-tools:jar:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-parent ---
[INFO] org.apache.beam:beam-sdks-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-parent ---
[INFO] org.apache.beam:beam-sdks-java-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Core 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-core ---
[INFO] org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.guava:guava-testlib:jar:19.0:test
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.errorprone:error_prone_annotations:jar:2.0.2:test
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (junit:junit:jar:4.11:test - version managed from 4.8.2; omitted for duplicate)
[INFO] +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  +- (org.tukaani:xz:jar:1.5:compile - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.codehaus.woodstox:stax2-api:jar:3.1.4:compile
[INFO] +- org.codehaus.woodstox:woodstox-core-asl:jar:4.4.1:runtime
[INFO] |  \- (org.codehaus.woodstox:stax2-api:jar:3.1.4:runtime - omitted for duplicate)
[INFO] +- org.tukaani:xz:jar:1.5:runtime (scope not updated to compile)
[INFO] +- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO] |  +- com.google.auto:auto-common:jar:0.3:compile
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:provided
[INFO] +- junit:junit:jar:4.11:provided
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:provided
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:jar:0.5.160304:test
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 3.0.0-beta-1; omitted for duplicate)
[INFO] \- com.esotericsoftware.kryo:kryo:jar:2.21:test
[INFO]    +- com.esotericsoftware.reflectasm:reflectasm:jar:shaded:1.07:test
[INFO]    |  \- org.ow2.asm:asm:jar:4.0:test
[INFO]    +- com.esotericsoftware.minlog:minlog:jar:1.2:test
[INFO]    \- org.objenesis:objenesis:jar:1.2:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-parent ---
[INFO] org.apache.beam:beam-runners-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Core Java 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-core-java ---
[INFO] org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] \- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Direct Java 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-direct-java ---
[INFO] org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.guava:guava-testlib:jar:19.0:test
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.errorprone:error_prone_annotations:jar:2.0.2:test
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (junit:junit:jar:4.11:test - version managed from 4.8.2; omitted for duplicate)
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO] |  +- com.google.auto:auto-common:jar:0.3:compile
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:provided
[INFO] +- junit:junit:jar:4.11:provided
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:provided
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-core-java:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.codehaus.woodstox:stax2-api:jar:3.1.4:test
[INFO] +- org.codehaus.woodstox:woodstox-core-asl:jar:4.4.1:test
[INFO] |  \- (org.codehaus.woodstox:stax2-api:jar:3.1.4:test - omitted for duplicate)
[INFO] \- com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:jar:0.5.160304:test
[INFO]    \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 3.0.0-beta-1; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Google Cloud Dataflow 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-google-cloud-dataflow-java ---
[INFO] org.apache.beam:beam-runners-google-cloud-dataflow-java:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-dataflow:jar:v1b3-rev43-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-clouddebugger:jar:v2-rev8-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |     \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.guava:guava-testlib:jar:19.0:test
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.errorprone:error_prone_annotations:jar:2.0.2:test
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (junit:junit:jar:4.11:test - version managed from 4.8.2; omitted for duplicate)
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO] |  +- com.google.auto:auto-common:jar:0.3:compile
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:provided
[INFO] +- junit:junit:jar:4.11:provided
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:provided
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- com.google.cloud.dataflow:google-cloud-dataflow-java-proto-library-all:jar:0.5.160304:test
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 3.0.0-beta-1; omitted for duplicate)
[INFO] \- com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:test
[INFO]    \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 3.0.0-beta-1; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-parent ---
[INFO] org.apache.beam:beam-sdks-java-io-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: Google Cloud Platform 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-google-cloud-platform ---
[INFO] org.apache.beam:beam-sdks-java-io-google-cloud-platform:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-protobuf:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |     \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:compile
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:compile
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] +- com.google.protobuf:protobuf-lite:jar:3.0.1:compile
[INFO] +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for duplicate)
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:compile
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  \- com.google.code.gson:gson:jar:2.3:compile
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  \- io.grpc:grpc-protobuf-lite:jar:1.0.1:compile
[INFO] |  |     +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |     \- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.cloud.bigtable:bigtable-client-core:jar:0.9.2:compile
[INFO] |  +- (com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for duplicate)
[INFO] |  +- commons-logging:commons-logging:jar:1.2:compile
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- com.google.auth:google-auth-library-appengine:jar:0.4.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- com.google.appengine:appengine-api-1.0-sdk:jar:1.9.34:compile
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- io.dropwizard.metrics:metrics-core:jar:3.1.2:compile
[INFO] |     \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.2)
[INFO] |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- io.netty:netty-tcnative-boringssl-static:jar:1.1.33.Fork18:runtime
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - version managed from 1.0.0; omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-google-cloud-dataflow-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-dataflow:jar:v1b3-rev43-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-clouddebugger:jar:v2-rev8-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] \- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: HDFS 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-hdfs ---
[INFO] org.apache.beam:beam-sdks-java-io-hdfs:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.2.4:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.1.2:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  |     \- (commons-codec:commons-codec:jar:1.3:compile - omitted for conflict with 1.9)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.avro:avro-mapred:jar:hadoop2:1.8.1:compile
[INFO] |  +- org.apache.avro:avro-ipc:jar:1.8.1:compile
[INFO] |  |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- org.mortbay.jetty:jetty:jar:6.1.26:compile
[INFO] |  |  |  \- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - omitted for duplicate)
[INFO] |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  |  +- io.netty:netty:jar:3.6.2.Final:compile
[INFO] |  |  +- org.apache.velocity:velocity:jar:1.7:compile
[INFO] |  |  |  +- commons-collections:commons-collections:jar:3.2.1:compile
[INFO] |  |  |  \- commons-lang:commons-lang:jar:2.6:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- commons-codec:commons-codec:jar:1.9:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.hadoop:hadoop-client:jar:2.7.0:provided
[INFO] |  +- (org.apache.hadoop:hadoop-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-hdfs:jar:2.7.0:provided
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:provided - omitted for duplicate)
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.2:provided - omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for conflict with 1.9)
[INFO] |  |  +- (commons-io:commons-io:jar:2.4:provided - omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.6:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.3:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  |  +- (xmlenc:xmlenc:jar:0.52:provided - omitted for duplicate)
[INFO] |  |  +- io.netty:netty-all:jar:4.0.23.Final:provided
[INFO] |  |  +- xerces:xercesImpl:jar:2.9.1:provided
[INFO] |  |  |  \- xml-apis:xml-apis:jar:1.3.04:provided
[INFO] |  |  +- (org.apache.htrace:htrace-core:jar:3.1.0-incubating:provided - omitted for duplicate)
[INFO] |  |  \- org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:provided
[INFO] |  +- org.apache.hadoop:hadoop-mapreduce-client-app:jar:2.7.0:provided
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.7.0:provided
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-yarn-client:jar:2.7.0:provided
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.1.3:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (commons-lang:commons-lang:jar:2.6:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (commons-cli:commons-cli:jar:1.2:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  |  \- (org.apache.hadoop:hadoop-yarn-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-yarn-server-common:jar:2.7.0:provided
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.1.3:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:provided - omitted for duplicate)
[INFO] |  |  |  |  \- (org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.7.0:provided
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-yarn-server-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.7.0:provided
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.6:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.3:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-mapreduce-client-jobclient:jar:2.7.0:provided
[INFO] |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  \- org.apache.hadoop:hadoop-annotations:jar:2.7.0:provided
[INFO] +- org.apache.hadoop:hadoop-common:jar:2.7.0:provided
[INFO] |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  +- commons-cli:commons-cli:jar:1.2:provided
[INFO] |  +- org.apache.commons:commons-math3:jar:3.1.1:provided
[INFO] |  +- xmlenc:xmlenc:jar:0.52:provided
[INFO] |  +- commons-httpclient:commons-httpclient:jar:3.1:provided
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.0.4:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  \- (commons-codec:commons-codec:jar:1.2:provided - omitted for conflict with 1.9)
[INFO] |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for conflict with 1.9)
[INFO] |  +- commons-io:commons-io:jar:2.4:provided
[INFO] |  +- commons-net:commons-net:jar:3.1:provided
[INFO] |  +- (commons-collections:commons-collections:jar:3.2.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- javax.servlet:servlet-api:jar:2.5:provided
[INFO] |  +- (org.mortbay.jetty:jetty:jar:6.1.26:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- javax.servlet.jsp:jsp-api:jar:2.1:provided
[INFO] |  +- com.sun.jersey:jersey-core:jar:1.9:provided
[INFO] |  +- com.sun.jersey:jersey-json:jar:1.9:provided
[INFO] |  |  +- org.codehaus.jettison:jettison:jar:1.1:provided
[INFO] |  |  +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:provided
[INFO] |  |  |  \- (javax.xml.bind:jaxb-api:jar:2.2.2:provided - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- org.codehaus.jackson:jackson-jaxrs:jar:1.8.3:provided
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- org.codehaus.jackson:jackson-xc:jar:1.8.3:provided
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-server:jar:1.9:provided
[INFO] |  |  +- asm:asm:jar:3.1:provided
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- (commons-logging:commons-logging:jar:1.1.3:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- log4j:log4j:jar:1.2.17:provided
[INFO] |  +- net.java.dev.jets3t:jets3t:jar:0.9.0:provided
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for conflict with 1.9)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (org.apache.httpcomponents:httpclient:jar:4.1.2:provided - omitted for conflict with 4.0.1)
[INFO] |  |  +- (org.apache.httpcomponents:httpcore:jar:4.1.2:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  \- com.jamesmurty.utils:java-xmlbuilder:jar:0.4:provided
[INFO] |  +- (commons-lang:commons-lang:jar:2.6:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- commons-configuration:commons-configuration:jar:1.6:provided
[INFO] |  |  +- (commons-collections:commons-collections:jar:3.2.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.4:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- commons-digester:commons-digester:jar:1.8:provided
[INFO] |  |  |  +- commons-beanutils:commons-beanutils:jar:1.7.0:provided
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:provided - omitted for conflict with 1.1.3)
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.1:provided - omitted for conflict with 1.1.3)
[INFO] |  |  \- commons-beanutils:commons-beanutils-core:jar:1.8.0:provided
[INFO] |  |     \- (commons-logging:commons-logging:jar:1.1.1:provided - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.10:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:provided - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (com.google.code.gson:gson:jar:2.2.4:runtime - scope updated from provided; omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-auth:jar:2.7.0:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for conflict with 1.9)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  |  +- (org.apache.httpcomponents:httpclient:jar:4.2.5:provided - omitted for conflict with 4.0.1)
[INFO] |  |  +- org.apache.directory.server:apacheds-kerberos-codec:jar:2.0.0-M15:provided
[INFO] |  |  |  +- org.apache.directory.server:apacheds-i18n:jar:2.0.0-M15:provided
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- org.apache.directory.api:api-asn1-api:jar:1.0.0-M20:provided
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- org.apache.directory.api:api-util:jar:1.0.0-M20:provided
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:provided - omitted for duplicate)
[INFO] |  |  \- org.apache.curator:curator-framework:jar:2.7.1:provided
[INFO] |  |     +- (org.apache.curator:curator-client:jar:2.7.1:provided - omitted for duplicate)
[INFO] |  |     +- (org.apache.zookeeper:zookeeper:jar:3.4.6:provided - omitted for duplicate)
[INFO] |  |     \- (com.google.guava:guava:jar:19.0:provided - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- com.jcraft:jsch:jar:0.1.42:provided
[INFO] |  +- org.apache.curator:curator-client:jar:2.7.1:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:provided - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:provided - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- org.apache.curator:curator-recipes:jar:2.7.1:provided
[INFO] |  |  +- (org.apache.curator:curator-framework:jar:2.7.1:provided - omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:provided - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:provided - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:provided - version managed from 3.0.0; omitted for duplicate)
[INFO] |  +- org.apache.htrace:htrace-core:jar:3.1.0-incubating:provided
[INFO] |  +- org.apache.zookeeper:zookeeper:jar:3.4.6:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:provided - omitted for conflict with 1.7.10)
[INFO] |  |  +- (log4j:log4j:jar:1.2.16:provided - omitted for conflict with 1.2.17)
[INFO] |  |  \- (io.netty:netty:jar:3.7.0.Final:provided - omitted for conflict with 3.6.2.Final)
[INFO] |  \- (org.apache.commons:commons-compress:jar:1.4.1:provided - omitted for conflict with 1.9)
[INFO] +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.0:provided
[INFO] |  +- org.apache.hadoop:hadoop-yarn-common:jar:2.7.0:provided
[INFO] |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  +- javax.xml.bind:jaxb-api:jar:2.2.2:provided
[INFO] |  |  |  +- javax.xml.stream:stax-api:jar:1.0-2:provided
[INFO] |  |  |  \- javax.activation:activation:jar:1.1:provided
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.4.1:provided - omitted for conflict with 1.9)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.6:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (javax.servlet:servlet-api:jar:2.5:provided - omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for conflict with 1.9)
[INFO] |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:provided - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- com.sun.jersey:jersey-client:jar:1.9:provided
[INFO] |  |  |  \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:provided - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-jaxrs:jar:1.9.13:provided - omitted for conflict with 1.8.3)
[INFO] |  |  +- (org.codehaus.jackson:jackson-xc:jar:1.9.13:provided - omitted for conflict with 1.8.3)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.3:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.2:provided - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (commons-io:commons-io:jar:2.4:provided - omitted for duplicate)
[INFO] |  |  +- com.google.inject:guice:jar:3.0:provided
[INFO] |  |  |  +- javax.inject:javax.inject:jar:1:provided
[INFO] |  |  |  \- aopalliance:aopalliance:jar:1.0:provided
[INFO] |  |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-json:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- com.sun.jersey.contribs:jersey-guice:jar:1.9:provided
[INFO] |  |  |  +- (javax.inject:javax.inject:jar:1:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  |  \- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:provided - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.10:provided - omitted for duplicate)
[INFO] |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.0:provided - omitted for duplicate)
[INFO] |  +- com.google.inject.extensions:guice-servlet:jar:3.0:provided
[INFO] |  |  \- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  \- (io.netty:netty:jar:3.6.2.Final:compile - scope updated from provided; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 3.0.0; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 11.0.2; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 3.0.0; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- junit:junit:jar:4.11:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: JMS 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-jms ---
[INFO] org.apache.beam:beam-sdks-java-io-jms:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- org.apache.geronimo.specs:geronimo-jms_1.1_spec:jar:1.1.1:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.apache.activemq:activemq-broker:jar:5.13.1:test
[INFO] |  +- (org.apache.activemq:activemq-client:jar:5.13.1:test - omitted for duplicate)
[INFO] |  \- org.apache.activemq:activemq-openwire-legacy:jar:5.13.1:test
[INFO] |     \- (org.apache.activemq:activemq-client:jar:5.13.1:test - omitted for duplicate)
[INFO] +- org.apache.activemq:activemq-kahadb-store:jar:5.13.1:test
[INFO] |  +- (org.apache.activemq:activemq-broker:jar:5.13.1:test - omitted for duplicate)
[INFO] |  +- org.apache.activemq.protobuf:activemq-protobuf:jar:1.1:test
[INFO] |  +- org.apache.geronimo.specs:geronimo-j2ee-management_1.1_spec:jar:1.0.1:test
[INFO] |  \- commons-net:commons-net:jar:3.3:test
[INFO] +- org.apache.activemq:activemq-client:jar:5.13.1:test
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.13; omitted for duplicate)
[INFO] |  +- (org.apache.geronimo.specs:geronimo-jms_1.1_spec:jar:1.1.1:test - omitted for duplicate)
[INFO] |  +- org.fusesource.hawtbuf:hawtbuf:jar:1.11:test
[INFO] |  \- (org.apache.geronimo.specs:geronimo-j2ee-management_1.1_spec:jar:1.0.1:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.13; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.13; omitted for duplicate)
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.13; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: Kafka 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-kafka ---
[INFO] org.apache.beam:beam-sdks-java-io-kafka:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.apache.kafka:kafka-clients:jar:0.9.0.1:compile
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.7:compile - omitted for conflict with 1.1.2.1)
[INFO] |  \- net.jpountz.lz4:lz4:jar:1.2.0:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.6; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] \- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.6; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: Kinesis 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-kinesis ---
[INFO] org.apache.beam:beam-sdks-java-io-kinesis:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- (org.apache.httpcomponents:httpclient:jar:4.0.1:compile - omitted for conflict with 4.5.2)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - version managed from 2.8.1; omitted for duplicate)
[INFO] +- com.amazonaws:aws-java-sdk-kinesis:jar:1.11.18:compile
[INFO] |  \- (com.amazonaws:aws-java-sdk-core:jar:1.11.18:compile - omitted for duplicate)
[INFO] +- com.amazonaws:amazon-kinesis-client:jar:1.6.1:compile
[INFO] |  +- (com.amazonaws:aws-java-sdk-core:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  +- com.amazonaws:aws-java-sdk-dynamodb:jar:1.10.20:compile
[INFO] |  |  +- com.amazonaws:aws-java-sdk-s3:jar:1.10.20:compile
[INFO] |  |  |  +- com.amazonaws:aws-java-sdk-kms:jar:1.10.20:compile
[INFO] |  |  |  |  \- (com.amazonaws:aws-java-sdk-core:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  |  |  \- (com.amazonaws:aws-java-sdk-core:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  |  \- (com.amazonaws:aws-java-sdk-core:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  +- (com.amazonaws:aws-java-sdk-kinesis:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  +- com.amazonaws:aws-java-sdk-cloudwatch:jar:1.10.20:compile
[INFO] |  |  \- (com.amazonaws:aws-java-sdk-core:jar:1.10.20:compile - omitted for conflict with 1.11.18)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  \- (commons-lang:commons-lang:jar:2.6:compile - omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- commons-lang:commons-lang:jar:2.6:compile
[INFO] +- com.amazonaws:aws-java-sdk-core:jar:1.11.18:compile
[INFO] |  +- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  +- org.apache.httpcomponents:httpclient:jar:4.5.2:compile
[INFO] |  |  +- org.apache.httpcomponents:httpcore:jar:4.4.4:compile
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.2:compile - omitted for conflict with 1.1.3)
[INFO] |  |  \- commons-codec:commons-codec:jar:1.9:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile - version managed from 2.6.6; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:jar:2.6.6:compile
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.6.6; omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - version managed from 2.8.1; omitted for duplicate)
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- org.assertj:assertj-core:jar:2.5.0:test
[INFO] +- com.google.guava:guava-testlib:jar:19.0:test
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.errorprone:error_prone_annotations:jar:2.0.2:test
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (junit:junit:jar:4.11:test - version managed from 4.8.2; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO]    +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO]    +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO]    |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO]    |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO]    |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO]    |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO]    |  +- (joda-time:joda-time:jar:2.4:test - version managed from 2.8.1; omitted for duplicate)
[INFO]    |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO]    +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO]    +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO]    +- (joda-time:joda-time:jar:2.4:test - version managed from 2.8.1; omitted for duplicate)
[INFO]    +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: MongoDB 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-mongodb ---
[INFO] org.apache.beam:beam-sdks-java-io-mongodb:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.mongodb:mongo-java-driver:jar:3.2.2:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- de.flapdoodle.embed:de.flapdoodle.embed.mongo:jar:1.50.1:test
[INFO] |  \- (de.flapdoodle.embed:de.flapdoodle.embed.process:jar:1.50.1:test - omitted for duplicate)
[INFO] +- de.flapdoodle.embed:de.flapdoodle.embed.process:jar:1.50.1:test
[INFO] |  +- commons-io:commons-io:jar:2.4:test
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.1:test
[INFO] |  +- net.java.dev.jna:jna:jar:4.0.0:test
[INFO] |  +- net.java.dev.jna:jna-platform:jar:4.0.0:test
[INFO] |  |  \- (net.java.dev.jna:jna:jar:4.0.0:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.3:test - omitted for conflict with 1.9)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.10; omitted for duplicate)
[INFO] \- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: IO :: JDBC 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-io-jdbc ---
[INFO] org.apache.beam:beam-sdks-java-io-jdbc:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.2)
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.apache.commons:commons-dbcp2:jar:2.1.1:compile
[INFO] |  +- org.apache.commons:commons-pool2:jar:2.4.2:compile
[INFO] |  \- commons-logging:commons-logging:jar:1.2:compile
[INFO] +- com.google.auto.value:auto-value:jar:1.1:provided
[INFO] +- org.apache.derby:derby:jar:10.12.1.1:test
[INFO] +- org.apache.derby:derbyclient:jar:10.12.1.1:test
[INFO] +- org.apache.derby:derbynet:jar:10.12.1.1:test
[INFO] |  \- (org.apache.derby:derby:jar:10.12.1.1:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- org.slf4j:slf4j-jdk14:jar:1.7.14:test
[INFO]    \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Maven Archetypes 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-maven-archetypes-parent ---
[INFO] org.apache.beam:beam-sdks-java-maven-archetypes-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Maven Archetypes :: Starter 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-maven-archetypes-starter ---
[INFO] org.apache.beam:beam-sdks-java-maven-archetypes-starter:maven-archetype:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:runtime
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:runtime
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:runtime
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:runtime
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:runtime
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:runtime
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:runtime
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:runtime
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:runtime
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:runtime
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:runtime
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:runtime
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:runtime
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:runtime
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:runtime
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:runtime
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:runtime
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:runtime
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:runtime
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:runtime (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:runtime (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:runtime - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:runtime
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.guava:guava:jar:19.0:runtime
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:runtime
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:runtime
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:runtime
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:runtime
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:runtime
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:runtime - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:runtime - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:runtime
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:runtime
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:runtime
[INFO] |  \- joda-time:joda-time:jar:2.4:runtime
[INFO] \- org.slf4j:slf4j-api:jar:1.7.14:runtime
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Maven Archetypes :: Examples 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-maven-archetypes-examples ---
[INFO] org.apache.beam:beam-sdks-java-maven-archetypes-examples:maven-archetype:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:runtime
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:runtime
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:runtime
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:runtime
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:runtime
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:runtime
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:runtime
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:runtime
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:runtime
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:runtime
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:runtime
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:runtime
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:runtime
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:runtime
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:runtime
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:runtime (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:runtime (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:runtime - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:runtime
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:runtime - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:runtime
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:runtime
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:runtime
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:runtime
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:runtime
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:runtime - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:runtime - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:runtime
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:runtime
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:runtime
[INFO] |  \- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-google-cloud-dataflow-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-dataflow:jar:v1b3-rev43-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-clouddebugger:jar:v2-rev8-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:runtime - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-io-google-cloud-platform:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:runtime - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:runtime - omitted for duplicate)
[INFO] |  +- com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:runtime
[INFO] |  |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:runtime
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:runtime
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-protobuf:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-client-core:jar:0.9.2:runtime
[INFO] |  |  +- (com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- commons-logging:commons-logging:jar:1.2:runtime
[INFO] |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- com.google.auth:google-auth-library-appengine:jar:0.4.0:runtime
[INFO] |  |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  \- com.google.appengine:appengine-api-1.0-sdk:jar:1.9.34:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-handler:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-transport:jar:4.1.3.Final:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- io.dropwizard.metrics:metrics-core:jar:3.1.2:runtime
[INFO] |  |     \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.netty:netty-tcnative-boringssl-static:jar:1.1.33.Fork18:runtime
[INFO] |  \- (org.apache.avro:avro:jar:1.8.1:runtime - omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:runtime
[INFO] +- joda-time:joda-time:jar:2.4:runtime
[INFO] +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:runtime
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:runtime
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:runtime
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:runtime
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:runtime
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:runtime
[INFO] |     +- (commons-logging:commons-logging:jar:1.1.1:runtime - omitted for conflict with 1.2)
[INFO] |     \- commons-codec:commons-codec:jar:1.3:runtime
[INFO] \- com.google.api-client:google-api-client:jar:1.22.0:runtime
[INFO]    +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO]    \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Extensions 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-extensions-parent ---
[INFO] org.apache.beam:beam-sdks-java-extensions-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Extensions :: Join library 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-extensions-join-library ---
[INFO] org.apache.beam:beam-sdks-java-extensions-join-library:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- junit:junit:jar:4.11:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Extensions :: Sorter 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-extensions-sorter ---
[INFO] org.apache.beam:beam-sdks-java-extensions-sorter:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- (com.google.code.gson:gson:jar:2.3:runtime - omitted for conflict with 2.2.4)
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- (org.apache.httpcomponents:httpcore:jar:4.0.1:compile - omitted for conflict with 4.1.2)
[INFO] |  |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |     \- (commons-codec:commons-codec:jar:1.3:compile - omitted for conflict with 1.4)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.1:compile
[INFO] |  +- org.apache.hadoop:hadoop-yarn-common:jar:2.7.1:compile
[INFO] |  |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.7.1:compile
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.6:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- (commons-logging:commons-logging:jar:1.1.3:compile - omitted for conflict with 1.1.1)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.1:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- javax.xml.bind:jaxb-api:jar:2.2.2:compile
[INFO] |  |  |  +- javax.xml.stream:stax-api:jar:1.0-2:compile
[INFO] |  |  |  \- javax.activation:activation:jar:1.1:compile
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.4.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.6:compile - omitted for duplicate)
[INFO] |  |  +- (javax.servlet:servlet-api:jar:2.5:compile - omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:compile - omitted for duplicate)
[INFO] |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  +- com.sun.jersey:jersey-client:jar:1.9:compile
[INFO] |  |  |  \- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- org.codehaus.jackson:jackson-jaxrs:jar:1.9.13:compile
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- org.codehaus.jackson:jackson-xc:jar:1.9.13:compile
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.3:compile - omitted for duplicate)
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.1:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (commons-io:commons-io:jar:2.4:compile - omitted for duplicate)
[INFO] |  |  +- com.google.inject:guice:jar:3.0:compile
[INFO] |  |  |  +- javax.inject:javax.inject:jar:1:compile
[INFO] |  |  |  \- aopalliance:aopalliance:jar:1.0:compile
[INFO] |  |  +- (com.sun.jersey:jersey-server:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-json:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  +- com.sun.jersey.contribs:jersey-guice:jar:1.9:compile
[INFO] |  |  |  +- (javax.inject:javax.inject:jar:1:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject:guice:jar:3.0:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.sun.jersey:jersey-server:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.10:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-annotations:jar:2.7.1:compile
[INFO] |  |  \- jdk.tools:jdk.tools:jar:1.8:system
[INFO] |  +- com.google.inject.extensions:guice-servlet:jar:3.0:compile
[INFO] |  |  \- (com.google.inject:guice:jar:3.0:compile - omitted for duplicate)
[INFO] |  \- io.netty:netty:jar:3.6.2.Final:compile
[INFO] +- org.apache.hadoop:hadoop-common:jar:2.7.1:compile
[INFO] |  +- (org.apache.hadoop:hadoop-annotations:jar:2.7.1:compile - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 11.0.2; omitted for duplicate)
[INFO] |  +- commons-cli:commons-cli:jar:1.2:compile
[INFO] |  +- org.apache.commons:commons-math3:jar:3.1.1:compile
[INFO] |  +- xmlenc:xmlenc:jar:0.52:compile
[INFO] |  +- commons-httpclient:commons-httpclient:jar:3.1:compile
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.0.4:compile - omitted for conflict with 1.1.3)
[INFO] |  |  \- (commons-codec:commons-codec:jar:1.2:compile - omitted for conflict with 1.4)
[INFO] |  +- commons-codec:commons-codec:jar:1.4:compile
[INFO] |  +- commons-io:commons-io:jar:2.4:compile
[INFO] |  +- commons-net:commons-net:jar:3.1:compile
[INFO] |  +- commons-collections:commons-collections:jar:3.2.1:compile
[INFO] |  +- javax.servlet:servlet-api:jar:2.5:compile
[INFO] |  +- org.mortbay.jetty:jetty:jar:6.1.26:compile
[INFO] |  |  \- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - omitted for duplicate)
[INFO] |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  +- javax.servlet.jsp:jsp-api:jar:2.1:runtime
[INFO] |  +- com.sun.jersey:jersey-core:jar:1.9:compile
[INFO] |  +- com.sun.jersey:jersey-json:jar:1.9:compile
[INFO] |  |  +- org.codehaus.jettison:jettison:jar:1.1:compile
[INFO] |  |  +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:compile
[INFO] |  |  |  \- (javax.xml.bind:jaxb-api:jar:2.2.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:compile - omitted for conflict with 1.9.13)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:compile - omitted for conflict with 1.9.13)
[INFO] |  |  +- (org.codehaus.jackson:jackson-jaxrs:jar:1.8.3:compile - omitted for conflict with 1.9.13)
[INFO] |  |  +- (org.codehaus.jackson:jackson-xc:jar:1.8.3:compile - omitted for conflict with 1.9.13)
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-server:jar:1.9:compile
[INFO] |  |  +- asm:asm:jar:3.1:compile
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  +- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  +- log4j:log4j:jar:1.2.17:compile
[INFO] |  +- net.java.dev.jets3t:jets3t:jar:0.9.0:compile
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:compile - omitted for duplicate)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  +- (org.apache.httpcomponents:httpclient:jar:4.1.2:compile - omitted for conflict with 4.0.1)
[INFO] |  |  +- org.apache.httpcomponents:httpcore:jar:4.1.2:compile
[INFO] |  |  \- com.jamesmurty.utils:java-xmlbuilder:jar:0.4:compile
[INFO] |  +- commons-lang:commons-lang:jar:2.6:compile
[INFO] |  +- commons-configuration:commons-configuration:jar:1.6:compile
[INFO] |  |  +- (commons-collections:commons-collections:jar:3.2.1:compile - omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.4:compile - omitted for conflict with 2.6)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  +- commons-digester:commons-digester:jar:1.8:compile
[INFO] |  |  |  +- commons-beanutils:commons-beanutils:jar:1.7.0:compile
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:compile - omitted for conflict with 1.1.3)
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  \- commons-beanutils:commons-beanutils-core:jar:1.8.0:compile
[INFO] |  |     \- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.10:runtime - omitted for duplicate)
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- com.google.code.gson:gson:jar:2.2.4:compile
[INFO] |  +- org.apache.hadoop:hadoop-auth:jar:2.7.1:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:compile - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.10:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.httpcomponents:httpclient:jar:4.2.5:compile - omitted for conflict with 4.0.1)
[INFO] |  |  +- org.apache.directory.server:apacheds-kerberos-codec:jar:2.0.0-M15:compile
[INFO] |  |  |  +- org.apache.directory.server:apacheds-i18n:jar:2.0.0-M15:compile
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- org.apache.directory.api:api-asn1-api:jar:1.0.0-M20:compile
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- org.apache.directory.api:api-util:jar:1.0.0-M20:compile
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - omitted for duplicate)
[INFO] |  |  \- org.apache.curator:curator-framework:jar:2.7.1:compile
[INFO] |  |     +- (org.apache.curator:curator-client:jar:2.7.1:compile - omitted for duplicate)
[INFO] |  |     +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - omitted for duplicate)
[INFO] |  |     \- (com.google.guava:guava:jar:19.0:compile - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- com.jcraft:jsch:jar:0.1.42:compile
[INFO] |  +- org.apache.curator:curator-client:jar:2.7.1:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- org.apache.curator:curator-recipes:jar:2.7.1:compile
[INFO] |  |  +- (org.apache.curator:curator-framework:jar:2.7.1:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for duplicate)
[INFO] |  +- org.apache.htrace:htrace-core:jar:3.1.0-incubating:compile
[INFO] |  +- org.apache.zookeeper:zookeeper:jar:3.4.6:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:compile - omitted for conflict with 1.7.10)
[INFO] |  |  +- (log4j:log4j:jar:1.2.16:compile - omitted for conflict with 1.2.17)
[INFO] |  |  \- (io.netty:netty:jar:3.7.0.Final:compile - omitted for conflict with 3.6.2.Final)
[INFO] |  \- (org.apache.commons:commons-compress:jar:1.4.1:compile - omitted for conflict with 1.9)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 3.0.0; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 16.0.1; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.6.1; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 3.0.0; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.6.1; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] \- junit:junit:jar:4.11:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: SDKs :: Java :: Java 8 Tests 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-sdks-java-java8tests ---
[INFO] org.apache.beam:beam-sdks-java-java8tests:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:test
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:test
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:test
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:test
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:test
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:test
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:test
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:test
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:test
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:test - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:test
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:test
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:test
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:test
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:test
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:test
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:test
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:test - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:test
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:test
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:test
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:test
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:test
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:test
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:test
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:test
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:test
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:test
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:test
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:test
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:test
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:test
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:test
[INFO] |  |     +- commons-logging:commons-logging:jar:1.1.1:test
[INFO] |  |     \- commons-codec:commons-codec:jar:1.3:test
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:test
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:test
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:test
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:test
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:test (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:test (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:test
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:test
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:test
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:test
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:test
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:test
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:test
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:test
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:test
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:test - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:test
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:test - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:test - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:test
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:test
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:test
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:test
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:test - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:test
[INFO] +- joda-time:joda-time:jar:2.4:test
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] \- junit:junit:jar:4.11:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Flink 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-flink-parent ---
[INFO] org.apache.beam:beam-runners-flink-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Flink :: Core 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-flink_2.10 ---
[INFO] org.apache.beam:beam-runners-flink_2.10:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.flink:flink-streaming-java_2.10:jar:1.1.2:compile
[INFO] |  +- org.apache.flink:flink-core:jar:1.1.2:compile
[INFO] |  |  +- org.apache.flink:flink-annotations:jar:1.1.2:compile
[INFO] |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- org.apache.flink:flink-metrics-core:jar:1.1.2:compile
[INFO] |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- com.esotericsoftware.kryo:kryo:jar:2.24.0:compile
[INFO] |  |  |  +- com.esotericsoftware.minlog:minlog:jar:1.2:compile
[INFO] |  |  |  \- org.objenesis:objenesis:jar:2.1:compile
[INFO] |  |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- org.apache.flink:flink-runtime_2.10:jar:1.1.2:compile
[INFO] |  |  +- (org.apache.flink:flink-core:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-java:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.3.1:compile - omitted for duplicate)
[INFO] |  |  +- io.netty:netty-all:jar:4.0.27.Final:compile
[INFO] |  |  +- org.javassist:javassist:jar:3.18.2-GA:compile
[INFO] |  |  +- org.scala-lang:scala-library:jar:2.10.4:compile
[INFO] |  |  +- com.typesafe.akka:akka-actor_2.10:jar:2.3.7:compile
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:compile - omitted for duplicate)
[INFO] |  |  |  \- com.typesafe:config:jar:1.2.1:compile
[INFO] |  |  +- com.typesafe.akka:akka-remote_2.10:jar:2.3.7:compile
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.7:compile - omitted for duplicate)
[INFO] |  |  |  +- io.netty:netty:jar:3.8.0.Final:compile
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  \- org.uncommons.maths:uncommons-maths:jar:1.2.2a:compile
[INFO] |  |  +- com.typesafe.akka:akka-slf4j_2.10:jar:2.3.7:compile
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.7:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- org.clapper:grizzled-slf4j_2.10:jar:1.0.2:compile
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.3:compile - omitted for conflict with 2.10.4)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- com.github.scopt:scopt_2.10:jar:3.2.0:compile
[INFO] |  |  +- io.dropwizard.metrics:metrics-core:jar:3.1.0:compile
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- io.dropwizard.metrics:metrics-jvm:jar:3.1.0:compile
[INFO] |  |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- io.dropwizard.metrics:metrics-json:jar:3.1.0:compile
[INFO] |  |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.0:compile - omitted for duplicate)
[INFO] |  |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile - version managed from 2.4.2; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- org.apache.zookeeper:zookeeper:jar:3.4.6:compile
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:compile - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.16:compile - omitted for conflict with 1.2.17)
[INFO] |  |  |  +- jline:jline:jar:0.9.94:compile
[INFO] |  |  |  |  \- (junit:junit:jar:4.11:test - version managed from 3.8.1; scope managed from compile; omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty:jar:3.7.0.Final:compile - omitted for conflict with 3.8.0.Final)
[INFO] |  |  +- com.twitter:chill_2.10:jar:0.7.4:compile
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.5:compile - omitted for conflict with 2.10.4)
[INFO] |  |  |  +- com.twitter:chill-java:jar:0.7.4:compile
[INFO] |  |  |  |  \- (com.esotericsoftware.kryo:kryo:jar:2.21:compile - omitted for conflict with 2.24.0)
[INFO] |  |  |  \- (com.esotericsoftware.kryo:kryo:jar:2.21:compile - omitted for conflict with 2.24.0)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-clients_2.10:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- org.apache.commons:commons-math3:jar:3.5:compile
[INFO] |  +- org.apache.sling:org.apache.sling.commons.json:jar:2.0.6:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.3.2:compile
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.7:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  +- log4j:log4j:jar:1.2.17:compile
[INFO] |  \- org.apache.flink:force-shading:jar:1.1.2:compile
[INFO] +- org.apache.flink:flink-java:jar:1.1.2:compile
[INFO] |  +- (org.apache.flink:flink-core:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:compile
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.3.1:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-math3:jar:3.5:compile - omitted for duplicate)
[INFO] |  |  +- xmlenc:xmlenc:jar:0.52:compile
[INFO] |  |  +- commons-codec:commons-codec:jar:1.4:compile
[INFO] |  |  +- commons-io:commons-io:jar:2.4:compile
[INFO] |  |  +- commons-net:commons-net:jar:3.1:compile
[INFO] |  |  +- commons-collections:commons-collections:jar:3.2.1:compile
[INFO] |  |  +- javax.servlet:servlet-api:jar:2.5:compile
[INFO] |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  |  +- com.sun.jersey:jersey-core:jar:1.9:compile
[INFO] |  |  +- commons-el:commons-el:jar:1.0:runtime
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:runtime - omitted for conflict with 1.1.3)
[INFO] |  |  +- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  |  +- com.jamesmurty.utils:java-xmlbuilder:jar:0.4:compile
[INFO] |  |  +- commons-lang:commons-lang:jar:2.6:compile
[INFO] |  |  +- commons-configuration:commons-configuration:jar:1.7:compile
[INFO] |  |  |  +- (commons-collections:commons-collections:jar:3.2.1:compile - omitted for duplicate)
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.6:compile - omitted for duplicate)
[INFO] |  |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  |  \- (commons-digester:commons-digester:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  |  +- commons-digester:commons-digester:jar:1.8.1:compile
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  +- org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile
[INFO] |  |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:compile
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.3:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.0.5:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- com.jcraft:jsch:jar:0.1.42:compile
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.4.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.0:compile
[INFO] |  |  +- commons-beanutils:commons-beanutils-bean-collections:jar:1.8.3:compile
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |  +- commons-daemon:commons-daemon:jar:1.0.13:compile
[INFO] |  |  +- javax.xml.bind:jaxb-api:jar:2.2.2:compile
[INFO] |  |  |  +- (javax.xml.stream:stax-api:jar:1.0-2:compile - omitted for duplicate)
[INFO] |  |  |  \- (javax.activation:activation:jar:1.1:compile - omitted for duplicate)
[INFO] |  |  +- javax.xml.stream:stax-api:jar:1.0-2:compile
[INFO] |  |  +- javax.activation:activation:jar:1.1:compile
[INFO] |  |  +- com.google.inject:guice:jar:3.0:compile
[INFO] |  |  |  +- (javax.inject:javax.inject:jar:1:compile - omitted for duplicate)
[INFO] |  |  |  \- (aopalliance:aopalliance:jar:1.0:compile - omitted for duplicate)
[INFO] |  |  +- javax.inject:javax.inject:jar:1:compile
[INFO] |  |  +- aopalliance:aopalliance:jar:1.0:compile
[INFO] |  |  +- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-math3:jar:3.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] +- org.apache.flink:flink-clients_2.10:jar:1.1.2:compile
[INFO] |  +- (org.apache.flink:flink-core:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- org.apache.flink:flink-optimizer_2.10:jar:1.1.2:compile
[INFO] |  |  +- (org.apache.flink:flink-core:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-java:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-java:jar:1.1.2:compile - omitted for duplicate)
[INFO] |  +- commons-cli:commons-cli:jar:1.3.1:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:compile - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:compile - omitted for duplicate)
[INFO] +- org.apache.flink:flink-core:test-jar:tests:1.1.2:test
[INFO] |  +- (org.apache.flink:flink-annotations:jar:1.1.2:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-metrics-core:jar:1.1.2:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.esotericsoftware.kryo:kryo:jar:2.24.0:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.6; scope updated from test; omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:test - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:test - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:test - omitted for duplicate)
[INFO] +- org.apache.flink:flink-runtime_2.10:test-jar:tests:1.1.2:test
[INFO] |  +- (org.apache.flink:flink-core:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-java:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (commons-cli:commons-cli:jar:1.3.1:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-all:jar:4.0.27.Final:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.javassist:javassist:jar:3.18.2-GA:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.scala-lang:scala-library:jar:2.10.4:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.7:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.typesafe.akka:akka-remote_2.10:jar:2.3.7:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.typesafe.akka:akka-slf4j_2.10:jar:2.3.7:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.clapper:grizzled-slf4j_2.10:jar:1.0.2:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.github.scopt:scopt_2.10:jar:3.2.0:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.0:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (io.dropwizard.metrics:metrics-jvm:jar:3.1.0:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (io.dropwizard.metrics:metrics-json:jar:3.1.0:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.twitter:chill_2.10:jar:0.7.4:compile - scope updated from test; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:test - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:test - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.1.3)
[INFO] |  |     \- (commons-codec:commons-codec:jar:1.3:compile - omitted for conflict with 1.4)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.guava:guava:jar:19.0:compile
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  |  +- (com.thoughtworks.paranamer:paranamer:jar:2.7:compile - omitted for conflict with 2.3)
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.0.5)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.4.1)
[INFO] |  |  +- (org.tukaani:xz:jar:1.5:compile - omitted for conflict with 1.0)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - version managed from 2.4.2; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] +- org.apache.flink:flink-streaming-java_2.10:test-jar:tests:1.1.2:test
[INFO] |  +- (org.apache.flink:flink-core:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-clients_2.10:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-math3:jar:3.5:test - omitted for duplicate)
[INFO] |  +- (org.apache.sling:org.apache.sling.commons.json:jar:2.0.6:test - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:test - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:test - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:test - omitted for duplicate)
[INFO] +- org.apache.flink:flink-test-utils_2.10:jar:1.1.2:test
[INFO] |  +- org.apache.flink:flink-test-utils-junit:jar:1.1.2:test
[INFO] |  |  +- (junit:junit:jar:4.11:test - version managed from 3.8.1; scope managed from compile; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:test - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:test - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:test - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-clients_2.10:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (org.apache.flink:flink-streaming-java_2.10:jar:1.1.2:test - omitted for duplicate)
[INFO] |  +- (junit:junit:jar:4.11:test - version managed from 3.8.1; scope managed from compile; omitted for duplicate)
[INFO] |  +- org.apache.curator:curator-test:jar:2.8.0:test
[INFO] |  |  +- (org.javassist:javassist:jar:3.18.1-GA:test - omitted for conflict with 3.18.2-GA)
[INFO] |  |  +- org.apache.commons:commons-math:jar:2.2:test
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:test - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:test - version managed from 16.0.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-lang3:jar:3.3.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:test - omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:test - omitted for duplicate)
[INFO] |  \- (org.apache.flink:force-shading:jar:1.1.2:test - omitted for duplicate)
[INFO] \- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO]    +- com.google.auto:auto-common:jar:0.3:compile
[INFO]    |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO]    \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Flink :: Examples 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-flink_2.10-examples ---
[WARNING] The parameter output is deprecated. Use outputFile instead.
[INFO] Wrote dependency tree to: /usr/local/google/home/davor/GitHub/incubator-beam/runners/flink/examples/wordcounts.txt
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Spark 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-spark ---
[INFO] org.apache.beam:beam-runners-spark:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.spark:spark-core_2.10:jar:1.6.2:provided
[INFO] |  +- (org.apache.avro:avro-mapred:jar:hadoop2:1.7.7:provided - omitted for conflict with 1.8.1)
[INFO] |  +- com.twitter:chill_2.10:jar:0.5.0:provided
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:provided - omitted for conflict with 2.10.5)
[INFO] |  |  +- (com.twitter:chill-java:jar:0.5.0:provided - omitted for duplicate)
[INFO] |  |  \- (com.esotericsoftware.kryo:kryo:jar:2.21:provided - omitted for duplicate)
[INFO] |  +- com.twitter:chill-java:jar:0.5.0:provided
[INFO] |  |  \- (com.esotericsoftware.kryo:kryo:jar:2.21:provided - omitted for duplicate)
[INFO] |  +- org.apache.xbean:xbean-asm5-shaded:jar:4.4:provided
[INFO] |  +- org.apache.hadoop:hadoop-client:jar:2.2.0:provided
[INFO] |  |  +- (org.apache.hadoop:hadoop-common:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-hdfs:jar:2.2.0:provided
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:provided - omitted for duplicate)
[INFO] |  |  |  +- (commons-cli:commons-cli:jar:1.2:provided - omitted for duplicate)
[INFO] |  |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for duplicate)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.1:provided - omitted for duplicate)
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.5:provided - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:provided - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (xmlenc:xmlenc:jar:0.52:provided - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-app:jar:2.2.0:provided
[INFO] |  |  |  +- org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.2.0:provided
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  +- org.apache.hadoop:hadoop-yarn-client:jar:2.2.0:provided
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  |  +- (commons-io:commons-io:jar:2.1:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  +- org.apache.hadoop:hadoop-yarn-server-common:jar:2.2.0:provided
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  |  +- (commons-io:commons-io:jar:2.1:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.2.0:provided
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:provided
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  \- (commons-io:commons-io:jar:2.1:provided - omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-jobclient:jar:2.2.0:provided
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for duplicate)
[INFO] |  |  \- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-launcher_2.10:jar:1.6.2:provided
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:provided - omitted for duplicate)
[INFO] |  +- (org.apache.spark:spark-network-common_2.10:jar:1.6.2:provided - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-network-shuffle_2.10:jar:1.6.2:provided
[INFO] |  |  +- (org.apache.spark:spark-network-common_2.10:jar:1.6.2:provided - omitted for duplicate)
[INFO] |  |  +- org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:provided
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:provided - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:provided - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-unsafe_2.10:jar:1.6.2:provided
[INFO] |  |  +- (com.twitter:chill_2.10:jar:0.5.0:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:provided - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:provided - omitted for duplicate)
[INFO] |  +- net.java.dev.jets3t:jets3t:jar:0.7.1:provided
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.3:provided - omitted for conflict with 1.4)
[INFO] |  |  \- (commons-httpclient:commons-httpclient:jar:3.1:provided - omitted for duplicate)
[INFO] |  +- org.apache.curator:curator-recipes:jar:2.4.0:provided
[INFO] |  |  +- org.apache.curator:curator-framework:jar:2.4.0:provided
[INFO] |  |  |  +- org.apache.curator:curator-client:jar:2.4.0:provided
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.6.4; omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:provided - omitted for duplicate)
[INFO] |  |  |  |  \- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:provided - omitted for duplicate)
[INFO] |  |  |  \- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:provided - omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  +- org.eclipse.jetty.orbit:javax.servlet:jar:3.0.0.v201112011016:provided
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.3.2:provided
[INFO] |  +- org.apache.commons:commons-math3:jar:3.4.1:provided
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:provided - version managed from 1.3.9; omitted for conflict with 1.3.9)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; scope updated from provided; omitted for duplicate)
[INFO] |  +- org.slf4j:jul-to-slf4j:jar:1.7.10:provided
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- org.slf4j:jcl-over-slf4j:jar:1.7.10:provided
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- log4j:log4j:jar:1.2.17:provided
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.10:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  +- com.ning:compress-lzf:jar:1.0.3:provided
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (net.jpountz.lz4:lz4:jar:1.3.0:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- org.roaringbitmap:RoaringBitmap:jar:0.5.11:provided
[INFO] |  +- commons-net:commons-net:jar:2.2:provided
[INFO] |  +- com.typesafe.akka:akka-remote_2.10:jar:2.3.11:provided
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:provided - omitted for duplicate)
[INFO] |  |  +- com.typesafe.akka:akka-actor_2.10:jar:2.3.11:provided
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:provided - omitted for duplicate)
[INFO] |  |  |  \- com.typesafe:config:jar:1.2.1:provided
[INFO] |  |  +- (io.netty:netty:jar:3.8.0.Final:provided - omitted for conflict with 3.6.2.Final)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  \- org.uncommons.maths:uncommons-maths:jar:1.2.2a:provided
[INFO] |  +- com.typesafe.akka:akka-slf4j_2.10:jar:2.3.11:provided
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:provided - omitted for duplicate)
[INFO] |  |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.11:provided - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  +- (org.scala-lang:scala-library:jar:2.10.5:provided - omitted for duplicate)
[INFO] |  +- org.json4s:json4s-jackson_2.10:jar:3.2.10:provided
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.0:provided - omitted for conflict with 2.10.5)
[INFO] |  |  +- org.json4s:json4s-core_2.10:jar:3.2.10:provided
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.0:provided - omitted for conflict with 2.10.5)
[INFO] |  |  |  +- org.json4s:json4s-ast_2.10:jar:3.2.10:provided
[INFO] |  |  |  |  \- (org.scala-lang:scala-library:jar:2.10.0:provided - omitted for conflict with 2.10.5)
[INFO] |  |  |  +- (com.thoughtworks.paranamer:paranamer:jar:2.6:provided - omitted for conflict with 2.7)
[INFO] |  |  |  \- org.scala-lang:scalap:jar:2.10.0:provided
[INFO] |  |  |     \- org.scala-lang:scala-compiler:jar:2.10.0:provided
[INFO] |  |  |        +- (org.scala-lang:scala-library:jar:2.10.0:provided - omitted for conflict with 2.10.5)
[INFO] |  |  |        \- (org.scala-lang:scala-reflect:jar:2.10.0:provided - omitted for conflict with 2.10.6)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.3.1; omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-server:jar:1.9:provided
[INFO] |  |  +- asm:asm:jar:3.1:provided
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-core:jar:1.9:provided
[INFO] |  +- org.apache.mesos:mesos:jar:shaded-protobuf:0.21.1:provided
[INFO] |  +- io.netty:netty-all:jar:4.0.29.Final:provided
[INFO] |  +- com.clearspring.analytics:stream:jar:2.7.0:provided
[INFO] |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:provided - omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-jvm:jar:3.1.2:provided
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:provided - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-json:jar:3.1.2:provided
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:provided - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.4.2; omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-graphite:jar:3.1.2:provided
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:provided - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.4.4; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.module:jackson-module-scala_2.10:jar:2.7.2:provided (version managed from 2.4.4)
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.6:provided - omitted for conflict with 2.10.5)
[INFO] |  |  +- org.scala-lang:scala-reflect:jar:2.10.6:provided
[INFO] |  |  |  \- (org.scala-lang:scala-library:jar:2.10.6:provided - omitted for conflict with 2.10.5)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:provided - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:provided - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  \- com.fasterxml.jackson.module:jackson-module-paranamer:jar:2.7.2:provided
[INFO] |  |     +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:provided - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |     \- (com.thoughtworks.paranamer:paranamer:jar:2.8:provided - omitted for conflict with 2.6)
[INFO] |  +- org.apache.ivy:ivy:jar:2.4.0:provided
[INFO] |  +- oro:oro:jar:2.0.8:provided
[INFO] |  +- org.tachyonproject:tachyon-client:jar:0.8.2:provided
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.4:provided - omitted for conflict with 2.5)
[INFO] |  |  +- (commons-io:commons-io:jar:2.4:provided - omitted for conflict with 2.1)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.0:provided - omitted for conflict with 3.3.2)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  +- org.tachyonproject:tachyon-underfs-hdfs:jar:0.8.2:provided
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.0:provided - omitted for conflict with 3.3.2)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  +- org.tachyonproject:tachyon-underfs-s3:jar:0.8.2:provided
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  \- org.tachyonproject:tachyon-underfs-local:jar:0.8.2:provided
[INFO] |  |     +- (com.google.guava:guava:jar:19.0:provided - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |     \- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.2; omitted for duplicate)
[INFO] |  +- net.razorvine:pyrolite:jar:4.9:provided
[INFO] |  +- net.sf.py4j:py4j:jar:0.9:provided
[INFO] |  \- org.spark-project.spark:unused:jar:1.0.0:provided
[INFO] +- org.apache.spark:spark-streaming_2.10:jar:1.6.2:provided
[INFO] |  +- (org.apache.spark:spark-core_2.10:jar:1.6.2:provided - omitted for duplicate)
[INFO] |  +- (org.scala-lang:scala-library:jar:2.10.5:provided - omitted for duplicate)
[INFO] |  \- (org.spark-project.spark:unused:jar:1.0.0:provided - omitted for duplicate)
[INFO] +- org.apache.spark:spark-network-common_2.10:jar:1.6.2:provided
[INFO] |  +- (io.netty:netty-all:jar:4.0.29.Final:provided - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:provided - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- (org.spark-project.spark:unused:jar:1.0.0:provided - omitted for duplicate)
[INFO] +- org.apache.hadoop:hadoop-common:jar:2.2.0:provided
[INFO] |  +- org.apache.hadoop:hadoop-annotations:jar:2.2.0:provided
[INFO] |  |  \- jdk.tools:jdk.tools:jar:1.6:system
[INFO] |  +- (com.google.guava:guava:jar:19.0:provided - version managed from 11.0.2; omitted for duplicate)
[INFO] |  +- commons-cli:commons-cli:jar:1.2:provided
[INFO] |  +- org.apache.commons:commons-math:jar:2.1:provided
[INFO] |  +- xmlenc:xmlenc:jar:0.52:provided
[INFO] |  +- commons-httpclient:commons-httpclient:jar:3.1:provided
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.0.4:provided - omitted for conflict with 1.1.1)
[INFO] |  |  \- (commons-codec:commons-codec:jar:1.2:provided - omitted for conflict with 1.3)
[INFO] |  +- (commons-codec:commons-codec:jar:1.4:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (commons-io:commons-io:jar:2.1:provided - omitted for conflict with 2.4)
[INFO] |  +- (commons-net:commons-net:jar:3.1:provided - omitted for conflict with 2.2)
[INFO] |  +- javax.servlet:servlet-api:jar:2.5:provided
[INFO] |  +- (org.mortbay.jetty:jetty:jar:6.1.26:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-json:jar:1.9:provided
[INFO] |  |  +- org.codehaus.jettison:jettison:jar:1.1:provided
[INFO] |  |  |  \- stax:stax-api:jar:1.0.1:provided
[INFO] |  |  +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:provided
[INFO] |  |  |  \- javax.xml.bind:jaxb-api:jar:2.2.2:provided
[INFO] |  |  |     \- javax.activation:activation:jar:1.1:provided
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- org.codehaus.jackson:jackson-jaxrs:jar:1.8.3:provided
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  +- org.codehaus.jackson:jackson-xc:jar:1.8.3:provided
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:provided - omitted for conflict with 1.9.13)
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- tomcat:jasper-compiler:jar:5.5.23:provided
[INFO] |  +- tomcat:jasper-runtime:jar:5.5.23:provided
[INFO] |  |  +- (javax.servlet:servlet-api:jar:2.4:provided - omitted for conflict with 2.5)
[INFO] |  |  \- (commons-el:commons-el:jar:1.0:provided - omitted for duplicate)
[INFO] |  +- javax.servlet.jsp:jsp-api:jar:2.1:provided
[INFO] |  +- commons-el:commons-el:jar:1.0:provided
[INFO] |  |  \- (commons-logging:commons-logging:jar:1.0.3:provided - omitted for conflict with 1.0.4)
[INFO] |  +- (commons-logging:commons-logging:jar:1.1.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  +- (net.java.dev.jets3t:jets3t:jar:0.6.1:provided - omitted for conflict with 0.7.1)
[INFO] |  +- (commons-lang:commons-lang:jar:2.5:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- commons-configuration:commons-configuration:jar:1.6:provided
[INFO] |  |  +- (commons-collections:commons-collections:jar:3.2.1:compile - scope updated from provided; omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.4:provided - omitted for conflict with 2.5)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:provided - omitted for duplicate)
[INFO] |  |  +- commons-digester:commons-digester:jar:1.8:provided
[INFO] |  |  |  +- commons-beanutils:commons-beanutils:jar:1.7.0:provided
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:provided - omitted for conflict with 1.1.1)
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.1:provided - omitted for conflict with 1.1.1)
[INFO] |  |  \- commons-beanutils:commons-beanutils-core:jar:1.8.0:provided
[INFO] |  |     \- (commons-logging:commons-logging:jar:1.1.1:provided - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for conflict with 1.7.10)
[INFO] |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:compile - scope updated from provided; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:provided - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; scope updated from provided; omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-auth:jar:2.2.0:provided
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:provided - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for conflict with 1.7.10)
[INFO] |  +- com.jcraft:jsch:jar:0.1.42:provided
[INFO] |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:provided - omitted for conflict with 3.4.6)
[INFO] |  \- (org.apache.commons:commons-compress:jar:1.4.1:compile - scope updated from provided; omitted for duplicate)
[INFO] +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:provided
[INFO] |  +- org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:provided
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:provided - omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for conflict with 1.7.10)
[INFO] |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:provided - omitted for duplicate)
[INFO] |  |  +- (io.netty:netty:jar:3.6.2.Final:provided - omitted for conflict with 3.8.0.Final)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  +- (commons-io:commons-io:jar:2.1:provided - omitted for duplicate)
[INFO] |  |  +- com.google.inject:guice:jar:3.0:provided
[INFO] |  |  |  +- javax.inject:javax.inject:jar:1:provided
[INFO] |  |  |  \- aopalliance:aopalliance:jar:1.0:provided
[INFO] |  |  +- com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:provided
[INFO] |  |  |  +- com.sun.jersey.jersey-test-framework:jersey-test-framework-core:jar:1.9:provided
[INFO] |  |  |  |  +- javax.servlet:javax.servlet-api:jar:3.0.1:provided
[INFO] |  |  |  |  +- (junit:junit:jar:4.11:test - version managed from 3.8.1; scope managed from provided; omitted for duplicate)
[INFO] |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  |  \- com.sun.jersey:jersey-client:jar:1.9:provided
[INFO] |  |  |  |     \- (com.sun.jersey:jersey-core:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  |  \- com.sun.jersey:jersey-grizzly2:jar:1.9:provided
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http:jar:2.1.2:provided
[INFO] |  |  |     |  \- org.glassfish.grizzly:grizzly-framework:jar:2.1.2:provided
[INFO] |  |  |     |     \- org.glassfish.gmbal:gmbal-api-only:jar:3.0.0-b023:provided
[INFO] |  |  |     |        \- org.glassfish.external:management-api:jar:3.0.0-b012:provided
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http-server:jar:2.1.2:provided
[INFO] |  |  |     |  +- (org.glassfish.grizzly:grizzly-http:jar:2.1.2:provided - omitted for duplicate)
[INFO] |  |  |     |  \- org.glassfish.grizzly:grizzly-rcm:jar:2.1.2:provided
[INFO] |  |  |     |     \- (org.glassfish.grizzly:grizzly-framework:jar:2.1.2:provided - omitted for duplicate)
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http-servlet:jar:2.1.2:provided
[INFO] |  |  |     |  +- (org.glassfish.grizzly:grizzly-framework:jar:2.1.2:provided - omitted for duplicate)
[INFO] |  |  |     |  \- (org.glassfish.grizzly:grizzly-http-server:jar:2.1.2:provided - omitted for duplicate)
[INFO] |  |  |     +- org.glassfish:javax.servlet:jar:3.1:provided
[INFO] |  |  |     \- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-json:jar:1.9:provided - omitted for duplicate)
[INFO] |  |  \- com.sun.jersey.contribs:jersey-guice:jar:1.9:provided
[INFO] |  |     +- (javax.inject:javax.inject:jar:1:provided - omitted for duplicate)
[INFO] |  |     +- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  |     +- (com.google.inject.extensions:guice-servlet:jar:3.0:provided - omitted for duplicate)
[INFO] |  |     \- (com.sun.jersey:jersey-server:jar:1.9:provided - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:provided - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:provided - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.7.5; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:provided - omitted for conflict with 1.7.10)
[INFO] |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:provided - omitted for duplicate)
[INFO] |  +- com.google.inject.extensions:guice-servlet:jar:3.0:provided
[INFO] |  |  \- (com.google.inject:guice:jar:3.0:provided - omitted for duplicate)
[INFO] |  \- (io.netty:netty:jar:3.6.2.Final:compile - scope updated from provided; omitted for duplicate)
[INFO] +- com.esotericsoftware.kryo:kryo:jar:2.21:provided
[INFO] |  +- com.esotericsoftware.reflectasm:reflectasm:jar:shaded:1.07:provided
[INFO] |  |  \- org.ow2.asm:asm:jar:4.0:provided
[INFO] |  +- com.esotericsoftware.minlog:minlog:jar:1.2:provided
[INFO] |  \- org.objenesis:objenesis:jar:1.2:provided
[INFO] +- de.javakaffee:kryo-serializers:jar:0.39:compile
[INFO] +- com.google.code.findbugs:jsr305:jar:1.3.9:compile
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO] |  +- com.google.auto:auto-common:jar:0.3:compile
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - omitted for duplicate)
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.4.1:compile - omitted for conflict with 1.9)
[INFO] |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for conflict with 1.3.9)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |     \- (commons-codec:commons-codec:jar:1.4:compile - omitted for conflict with 1.9)
[INFO] +- org.apache.commons:commons-compress:jar:1.9:provided (scope not updated to compile)
[INFO] +- commons-io:commons-io:jar:2.4:provided
[INFO] +- org.apache.zookeeper:zookeeper:jar:3.4.6:provided
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:provided - version managed from 1.6.1; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:provided - omitted for conflict with 1.7.10)
[INFO] |  +- (log4j:log4j:jar:1.2.16:provided - omitted for conflict with 1.2.17)
[INFO] |  +- jline:jline:jar:0.9.94:provided
[INFO] |  |  \- (junit:junit:jar:4.11:test - version managed from 3.8.1; scope managed from provided; omitted for duplicate)
[INFO] |  \- (io.netty:netty:jar:3.7.0.Final:provided - omitted for conflict with 3.6.2.Final)
[INFO] +- org.scala-lang:scala-library:jar:2.10.5:provided
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 3.0.0; omitted for conflict with 1.3.9)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for conflict with 1.3.9)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile - version managed from 2.4.4; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.1; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:compile - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:compile - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.1; omitted for duplicate)
[INFO] +- org.apache.avro:avro-mapred:jar:hadoop2:1.8.1:compile
[INFO] |  +- org.apache.avro:avro-ipc:jar:1.8.1:compile
[INFO] |  |  +- (org.apache.avro:avro:jar:1.8.1:compile - version managed from 1.7.4; omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  |  +- org.mortbay.jetty:jetty:jar:6.1.26:compile
[INFO] |  |  |  \- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - omitted for duplicate)
[INFO] |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  |  +- io.netty:netty:jar:3.6.2.Final:compile
[INFO] |  |  +- org.apache.velocity:velocity:jar:1.7:compile
[INFO] |  |  |  +- commons-collections:commons-collections:jar:3.2.1:compile
[INFO] |  |  |  \- commons-lang:commons-lang:jar:2.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for conflict with 1.8.8)
[INFO] |  +- commons-codec:commons-codec:jar:1.9:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- io.dropwizard.metrics:metrics-core:jar:3.1.2:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-io-kafka:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (org.apache.kafka:kafka-clients:jar:0.9.0.1:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] +- org.apache.kafka:kafka-clients:jar:0.9.0.1:compile
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.7:compile - omitted for conflict with 1.1.2.1)
[INFO] |  \- net.jpountz.lz4:lz4:jar:1.2.0:compile
[INFO] +- junit:junit:jar:4.11:provided
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:provided
[INFO] +- org.apache.kafka:kafka_2.10:jar:0.9.0.1:test
[INFO] |  +- com.101tec:zkclient:jar:0.7:test
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:test - omitted for conflict with 1.7.10)
[INFO] |  |  +- (log4j:log4j:jar:1.2.15:test - omitted for conflict with 1.2.17)
[INFO] |  |  \- (org.apache.zookeeper:zookeeper:jar:3.4.6:test - omitted for duplicate)
[INFO] |  +- (org.apache.kafka:kafka-clients:jar:0.9.0.1:test - omitted for duplicate)
[INFO] |  +- com.yammer.metrics:metrics-core:jar:2.2.0:test
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.2; omitted for duplicate)
[INFO] |  +- (org.scala-lang:scala-library:jar:2.10.5:test - omitted for duplicate)
[INFO] |  +- net.sf.jopt-simple:jopt-simple:jar:3.2:test
[INFO] |  +- (org.slf4j:slf4j-log4j12:jar:1.7.6:test - omitted for conflict with 1.7.10)
[INFO] |  \- (org.apache.zookeeper:zookeeper:jar:3.4.6:test - omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for conflict with 1.3.9)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - version managed from 2.4.4; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.2; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - omitted for duplicate)
[INFO] \- org.mockito:mockito-all:jar:1.9.5:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Runners :: Apex 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-runners-apex ---
[INFO] org.apache.beam:beam-runners-apex:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.apex:apex-common:jar:3.5.0-SNAPSHOT:compile
[INFO] |  +- org.apache.apex:apex-api:jar:3.5.0-SNAPSHOT:compile
[INFO] |  |  +- org.apache.hadoop:hadoop-common:jar:2.2.0:compile
[INFO] |  |  |  +- org.apache.hadoop:hadoop-annotations:jar:2.2.0:compile
[INFO] |  |  |  |  \- jdk.tools:jdk.tools:jar:1.6:system
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- commons-cli:commons-cli:jar:1.2:compile
[INFO] |  |  |  +- org.apache.commons:commons-math:jar:2.1:compile
[INFO] |  |  |  +- xmlenc:xmlenc:jar:0.52:compile
[INFO] |  |  |  +- commons-httpclient:commons-httpclient:jar:3.1:compile
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.0.4:compile - omitted for conflict with 1.1.1)
[INFO] |  |  |  |  \- (commons-codec:commons-codec:jar:1.2:compile - omitted for conflict with 1.10)
[INFO] |  |  |  +- commons-codec:commons-codec:jar:1.10:compile
[INFO] |  |  |  +- commons-io:commons-io:jar:2.1:compile
[INFO] |  |  |  +- commons-net:commons-net:jar:3.1:compile
[INFO] |  |  |  +- javax.servlet:servlet-api:jar:2.5:compile
[INFO] |  |  |  +- org.mortbay.jetty:jetty:jar:6.1.26:compile
[INFO] |  |  |  |  \- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - omitted for duplicate)
[INFO] |  |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:compile
[INFO] |  |  |  +- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  |  +- com.sun.jersey:jersey-json:jar:1.9:compile
[INFO] |  |  |  |  +- org.codehaus.jettison:jettison:jar:1.1:compile
[INFO] |  |  |  |  |  \- stax:stax-api:jar:1.0.1:compile
[INFO] |  |  |  |  +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:compile
[INFO] |  |  |  |  |  \- javax.xml.bind:jaxb-api:jar:2.2.2:compile
[INFO] |  |  |  |  |     \- (javax.activation:activation:jar:1.1:compile - omitted for duplicate)
[INFO] |  |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.3:compile - omitted for conflict with 1.8.8)
[INFO] |  |  |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.3:compile - omitted for conflict with 1.8.8)
[INFO] |  |  |  |  \- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  |  |  +- (tomcat:jasper-compiler:jar:5.5.23:runtime - omitted for duplicate)
[INFO] |  |  |  +- (tomcat:jasper-runtime:jar:5.5.23:runtime - omitted for duplicate)
[INFO] |  |  |  +- (javax.servlet.jsp:jsp-api:jar:2.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-el:commons-el:jar:1.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for duplicate)
[INFO] |  |  |  +- log4j:log4j:jar:1.2.17:compile
[INFO] |  |  |  +- net.java.dev.jets3t:jets3t:jar:0.6.1:compile
[INFO] |  |  |  |  +- (commons-codec:commons-codec:jar:1.3:compile - omitted for conflict with 1.10)
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for duplicate)
[INFO] |  |  |  |  \- (commons-httpclient:commons-httpclient:jar:3.1:compile - omitted for duplicate)
[INFO] |  |  |  +- commons-lang:commons-lang:jar:2.5:compile
[INFO] |  |  |  +- commons-configuration:commons-configuration:jar:1.6:compile
[INFO] |  |  |  |  +- commons-collections:commons-collections:jar:3.2.1:compile
[INFO] |  |  |  |  +- (commons-lang:commons-lang:jar:2.4:compile - omitted for conflict with 2.5)
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for duplicate)
[INFO] |  |  |  |  \- commons-digester:commons-digester:jar:1.8:compile
[INFO] |  |  |  |     +- (commons-beanutils:commons-beanutils:jar:1.7.0:compile - omitted for conflict with 1.8.3)
[INFO] |  |  |  |     \- (commons-logging:commons-logging:jar:1.1:compile - omitted for conflict with 1.1.1)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.6; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:compile - omitted for conflict with 1.9.13)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:compile - omitted for conflict with 1.9.13)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-auth:jar:2.2.0:compile
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  +- (commons-codec:commons-codec:jar:1.4:compile - omitted for duplicate)
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for duplicate)
[INFO] |  |  |  +- com.jcraft:jsch:jar:0.1.42:compile
[INFO] |  |  |  +- org.apache.zookeeper:zookeeper:jar:3.4.5:compile
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:compile - omitted for duplicate)
[INFO] |  |  |  |  \- (log4j:log4j:jar:1.2.15:compile - omitted for conflict with 1.2.17)
[INFO] |  |  |  \- (org.apache.commons:commons-compress:jar:1.4.1:compile - omitted for conflict with 1.8.1)
[INFO] |  |  \- com.datatorrent:netlet:jar:1.2.1:compile
[INFO] |  |     +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |     \- org.slf4j:slf4j-log4j12:jar:1.7.5:compile
[INFO] |  |        +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |        \- (log4j:log4j:jar:1.2.17:compile - omitted for duplicate)
[INFO] |  +- com.esotericsoftware.kryo:kryo:jar:2.24.0:compile
[INFO] |  |  +- com.esotericsoftware.minlog:minlog:jar:1.2:compile
[INFO] |  |  \- org.objenesis:objenesis:jar:2.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- javax.validation:validation-api:jar:1.1.0.Final:compile
[INFO] |  \- com.sun.jersey:jersey-core:jar:1.9:compile
[INFO] +- org.apache.apex:malhar-library:jar:3.4.0:compile
[INFO] |  +- (org.apache.apex:apex-common:jar:3.4.0:compile - omitted for conflict with 3.5.0-SNAPSHOT)
[INFO] |  +- org.eclipse.jetty:jetty-servlet:jar:8.1.10.v20130312:compile
[INFO] |  |  \- org.eclipse.jetty:jetty-security:jar:8.1.10.v20130312:compile
[INFO] |  |     \- org.eclipse.jetty:jetty-server:jar:8.1.10.v20130312:compile
[INFO] |  |        +- org.eclipse.jetty:jetty-continuation:jar:8.1.10.v20130312:compile
[INFO] |  |        \- (org.eclipse.jetty:jetty-http:jar:8.1.10.v20130312:compile - omitted for duplicate)
[INFO] |  +- com.sun.mail:javax.mail:jar:1.5.0:compile
[INFO] |  |  \- javax.activation:activation:jar:1.1:compile
[INFO] |  +- com.sun.jersey:jersey-client:jar:1.9:compile
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:compile - omitted for duplicate)
[INFO] |  +- javax.jms:jms-api:jar:1.1-rev-1:compile
[INFO] |  +- org.apache.activemq:activemq-client:jar:5.8.0:compile
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- org.apache.geronimo.specs:geronimo-jms_1.1_spec:jar:1.1.1:compile
[INFO] |  |  +- org.fusesource.hawtbuf:hawtbuf:jar:1.9:compile
[INFO] |  |  \- org.apache.geronimo.specs:geronimo-j2ee-management_1.1_spec:jar:1.0.1:compile
[INFO] |  +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] |  +- com.github.tony19:named-regexp:jar:0.2.3:compile
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.1:compile
[INFO] |  +- org.codehaus.janino:commons-compiler:jar:2.7.8:compile
[INFO] |  +- org.eclipse.jetty:jetty-websocket:jar:8.1.10.v20130312:compile
[INFO] |  |  +- org.eclipse.jetty:jetty-util:jar:8.1.10.v20130312:compile
[INFO] |  |  +- org.eclipse.jetty:jetty-io:jar:8.1.10.v20130312:compile
[INFO] |  |  |  \- (org.eclipse.jetty:jetty-util:jar:8.1.10.v20130312:compile - omitted for duplicate)
[INFO] |  |  \- org.eclipse.jetty:jetty-http:jar:8.1.10.v20130312:compile
[INFO] |  |     \- (org.eclipse.jetty:jetty-io:jar:8.1.10.v20130312:compile - omitted for duplicate)
[INFO] |  +- commons-beanutils:commons-beanutils:jar:1.8.3:compile
[INFO] |  |  \- commons-logging:commons-logging:jar:1.1.1:compile
[INFO] |  +- joda-time:joda-time:jar:2.4:compile (version managed from 2.9.1)
[INFO] |  +- it.unimi.dsi:fastutil:jar:7.0.6:compile
[INFO] |  \- org.apache.apex:apex-shaded-ning19:jar:1.0.0:compile
[INFO] +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile (version managed from 2.7.0)
[INFO] |  \- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] +- org.apache.apex:apex-engine:jar:3.5.0-SNAPSHOT:runtime
[INFO] |  +- org.apache.bval:bval-jsr303:jar:0.5:runtime
[INFO] |  |  +- org.apache.bval:bval-core:jar:0.5:runtime
[INFO] |  |  |  \- (org.apache.commons:commons-lang3:jar:3.1:runtime - omitted for duplicate)
[INFO] |  |  \- (org.apache.commons:commons-lang3:jar:3.1:runtime - omitted for duplicate)
[INFO] |  +- org.apache.apex:apex-bufferserver:jar:3.5.0-SNAPSHOT:runtime
[INFO] |  |  \- (org.apache.apex:apex-common:jar:3.5.0-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- (org.apache.httpcomponents:httpclient:jar:4.3.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.sun.jersey.contribs:jersey-apache-client4:jar:1.9:runtime
[INFO] |  |  +- (org.apache.httpcomponents:httpclient:jar:4.1.1:runtime - omitted for conflict with 4.3.5)
[INFO] |  |  \- (com.sun.jersey:jersey-client:jar:1.9:runtime - omitted for duplicate)
[INFO] |  +- org.apache.hadoop:hadoop-yarn-client:jar:2.2.0:runtime
[INFO] |  |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:runtime
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (io.netty:netty:jar:3.6.2.Final:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:runtime
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject.extensions:guice-servlet:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (io.netty:netty:jar:3.6.2.Final:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- com.google.inject.extensions:guice-servlet:jar:3.0:runtime
[INFO] |  |  |  \- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  +- io.netty:netty:jar:3.6.2.Final:runtime
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-io:commons-io:jar:2.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- com.google.inject:guice:jar:3.0:runtime
[INFO] |  |  |  +- javax.inject:javax.inject:jar:1:runtime
[INFO] |  |  |  \- aopalliance:aopalliance:jar:1.0:runtime
[INFO] |  |  +- com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:runtime
[INFO] |  |  |  +- com.sun.jersey.jersey-test-framework:jersey-test-framework-core:jar:1.9:runtime
[INFO] |  |  |  |  +- javax.servlet:javax.servlet-api:jar:3.0.1:runtime
[INFO] |  |  |  |  +- (junit:junit:jar:4.11:test - version managed from 4.8.2; scope managed from runtime; omitted for duplicate)
[INFO] |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (com.sun.jersey:jersey-client:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  \- com.sun.jersey:jersey-grizzly2:jar:1.9:runtime
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http:jar:2.1.2:runtime
[INFO] |  |  |     |  \- org.glassfish.grizzly:grizzly-framework:jar:2.1.2:runtime
[INFO] |  |  |     |     \- org.glassfish.gmbal:gmbal-api-only:jar:3.0.0-b023:runtime
[INFO] |  |  |     |        \- org.glassfish.external:management-api:jar:3.0.0-b012:runtime
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http-server:jar:2.1.2:runtime
[INFO] |  |  |     |  +- (org.glassfish.grizzly:grizzly-http:jar:2.1.2:runtime - omitted for duplicate)
[INFO] |  |  |     |  \- org.glassfish.grizzly:grizzly-rcm:jar:2.1.2:runtime
[INFO] |  |  |     |     \- (org.glassfish.grizzly:grizzly-framework:jar:2.1.2:runtime - omitted for duplicate)
[INFO] |  |  |     +- org.glassfish.grizzly:grizzly-http-servlet:jar:2.1.2:runtime
[INFO] |  |  |     |  +- (org.glassfish.grizzly:grizzly-framework:jar:2.1.2:runtime - omitted for duplicate)
[INFO] |  |  |     |  \- (org.glassfish.grizzly:grizzly-http-server:jar:2.1.2:runtime - omitted for duplicate)
[INFO] |  |  |     +- org.glassfish:javax.servlet:jar:3.1:runtime
[INFO] |  |  |     \- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- com.sun.jersey:jersey-server:jar:1.9:runtime
[INFO] |  |  |  +- asm:asm:jar:3.1:runtime
[INFO] |  |  |  \- (com.sun.jersey:jersey-core:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-json:jar:1.9:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  \- com.sun.jersey.contribs:jersey-guice:jar:1.9:runtime
[INFO] |  |     +- (javax.inject:javax.inject:jar:1:runtime - omitted for duplicate)
[INFO] |  |     +- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |     +- (com.google.inject.extensions:guice-servlet:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |     \- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  +- jline:jline:jar:2.11:runtime
[INFO] |  +- org.apache.ant:ant:jar:1.9.2:runtime
[INFO] |  |  \- org.apache.ant:ant-launcher:jar:1.9.2:runtime
[INFO] |  +- net.engio:mbassador:jar:1.1.9:runtime
[INFO] |  +- org.apache.hadoop:hadoop-common:test-jar:tests:2.2.0:runtime
[INFO] |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 11.0.2; scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-cli:commons-cli:jar:1.2:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-math:jar:2.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (xmlenc:xmlenc:jar:0.52:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-httpclient:commons-httpclient:jar:3.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.4:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-io:commons-io:jar:2.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-net:commons-net:jar:3.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (javax.servlet:servlet-api:jar:2.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.mortbay.jetty:jetty:jar:6.1.26:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-core:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-json:jar:1.9:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  +- tomcat:jasper-compiler:jar:5.5.23:runtime
[INFO] |  |  +- tomcat:jasper-runtime:jar:5.5.23:runtime
[INFO] |  |  |  +- (javax.servlet:servlet-api:jar:2.4:runtime - omitted for conflict with 2.5)
[INFO] |  |  |  \- (commons-el:commons-el:jar:1.0:runtime - omitted for duplicate)
[INFO] |  |  +- javax.servlet.jsp:jsp-api:jar:2.1:runtime
[INFO] |  |  +- commons-el:commons-el:jar:1.0:runtime
[INFO] |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:runtime - omitted for conflict with 1.1.1)
[INFO] |  |  +- (commons-logging:commons-logging:jar:1.1.1:runtime - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (net.java.dev.jets3t:jets3t:jar:0.6.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-lang:commons-lang:jar:2.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (commons-configuration:commons-configuration:jar:1.6:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.4; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.apache.hadoop:hadoop-auth:jar:2.2.0:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (com.jcraft:jsch:jar:0.1.42:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  |  \- (org.apache.commons:commons-compress:jar:1.4.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- net.lingala.zip4j:zip4j:jar:1.3.2:runtime
[INFO] |  +- (commons-beanutils:commons-beanutils:jar:1.8.3:runtime - omitted for duplicate)
[INFO] |  +- (commons-codec:commons-codec:jar:1.10:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- (org.eclipse.jetty:jetty-websocket:jar:8.1.10.v20130312:runtime - omitted for duplicate)
[INFO] |  +- org.apache.xbean:xbean-asm5-shaded:jar:4.3:runtime
[INFO] |  +- org.jctools:jctools-core:jar:1.1:runtime
[INFO] |  \- (org.apache.apex:apex-shaded-ning19:jar:1.0.0:runtime - omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:runtime
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:runtime
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:runtime
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |  |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |  |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for duplicate)
[INFO] |  |     \- (commons-codec:commons-codec:jar:1.3:compile - omitted for conflict with 1.10)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:runtime
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.guava:guava:jar:19.0:compile
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - version managed from 2.9.1; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - version managed from 2.9.1; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:tests:0.4.0-incubating-SNAPSHOT:test
[INFO] |  +- (io.grpc:grpc-auth:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-netty:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-stub:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-all:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:test - omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:test - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:test - omitted for duplicate)
[INFO] |  +- (io.netty:netty-handler:jar:4.1.3.Final:test - omitted for duplicate)
[INFO] |  +- (com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:test - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:test - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:test - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:test - omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:test - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:gcsio:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:test - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:test - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:test - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:test - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:test - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:test - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:test - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:test - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (net.bytebuddy:byte-buddy:jar:1.4.3:test - omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:test - version managed from 1.7.4; omitted for duplicate)
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:test - omitted for duplicate)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.9:test - omitted for duplicate)
[INFO] |  \- (joda-time:joda-time:jar:2.4:test - version managed from 2.9.1; omitted for duplicate)
[INFO] \- com.google.auto.service:auto-service:jar:1.0-rc2:compile
[INFO]    +- com.google.auto:auto-common:jar:0.3:compile
[INFO]    |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO]    \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Examples 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-examples-parent ---
[INFO] org.apache.beam:beam-examples-parent:pom:0.4.0-incubating-SNAPSHOT
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Examples :: Java 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-examples-java ---
[INFO] org.apache.beam:beam-examples-java:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-protobuf:jar:1.0.1:compile - version managed from 1.0.0; scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-io-google-cloud-platform:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:compile
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:compile
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:compile
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:compile
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  |  \- io.grpc:grpc-protobuf-lite:jar:1.0.1:compile
[INFO] |  |  |     +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |     \- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-client-core:jar:0.9.2:compile
[INFO] |  |  +- (com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- commons-logging:commons-logging:jar:1.2:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- com.google.auth:google-auth-library-appengine:jar:0.4.0:compile
[INFO] |  |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  \- com.google.appengine:appengine-api-1.0-sdk:jar:1.9.34:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- io.dropwizard.metrics:metrics-core:jar:3.1.2:compile
[INFO] |  |     \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.netty:netty-tcnative-boringssl-static:jar:1.1.33.Fork18:runtime
[INFO] |  \- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.2)
[INFO] |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:compile
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] +- com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile
[INFO] |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:runtime
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] \- org.mockito:mockito-all:jar:1.9.5:test
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building Apache Beam :: Examples :: Java 8 0.4.0-incubating-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.10:tree (default-cli) @ beam-examples-java8 ---
[INFO] org.apache.beam:beam-examples-java8:jar:0.4.0-incubating-SNAPSHOT
[INFO] +- org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- io.grpc:grpc-auth:jar:1.0.1:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-core:jar:1.0.1:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-context:jar:1.0.1:compile
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.grpc:grpc-netty:jar:1.0.1:compile
[INFO] |  |  +- io.netty:netty-codec-http2:jar:4.1.3.Final:compile
[INFO] |  |  |  +- io.netty:netty-codec-http:jar:4.1.3.Final:compile
[INFO] |  |  |  |  \- (io.netty:netty-codec:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-stub:jar:1.0.1:compile
[INFO] |  |  \- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- io.grpc:grpc-all:jar:1.0.1:runtime
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-context:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-protobuf:jar:1.0.1:compile - version managed from 1.0.0; scope updated from runtime; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf-nano:jar:1.0.1:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf.nano:protobuf-javanano:jar:3.0.0-alpha-5:runtime
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-okhttp:jar:1.0.1:runtime
[INFO] |  |  |  +- com.squareup.okio:okio:jar:1.6.0:runtime
[INFO] |  |  |  +- com.squareup.okhttp:okhttp:jar:2.5.0:runtime
[INFO] |  |  |  |  \- (com.squareup.okio:okio:jar:1.6.0:runtime - omitted for duplicate)
[INFO] |  |  |  \- (io.grpc:grpc-core:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  |  \- (io.grpc:grpc-protobuf-lite:jar:1.0.1:runtime - omitted for duplicate)
[INFO] |  +- (io.grpc:grpc-protobuf-lite:jar:1.0.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.auth:google-auth-library-credentials:jar:0.6.0:compile
[INFO] |  +- com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- io.netty:netty-handler:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-buffer:jar:4.1.3.Final:compile
[INFO] |  |  |  \- io.netty:netty-common:jar:4.1.3.Final:compile
[INFO] |  |  +- io.netty:netty-transport:jar:4.1.3.Final:compile
[INFO] |  |  |  +- (io.netty:netty-buffer:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  |  \- io.netty:netty-resolver:jar:4.1.3.Final:compile
[INFO] |  |  |     \- (io.netty:netty-common:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  \- io.netty:netty-codec:jar:4.1.3.Final:compile
[INFO] |  |     \- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  +- com.google.api.grpc:grpc-google-pubsub-v1:jar:0.1.0:compile
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.api.grpc:grpc-google-common-protos:jar:0.1.0:compile
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  |  \- com.google.api.grpc:grpc-google-iam-v1:jar:0.1.0:compile
[INFO] |  |     \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-cloudresourcemanager:jar:v1-rev6-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile - omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:gcsio:jar:1.4.5:compile
[INFO] |  |  +- com.google.api-client:google-api-client-java6:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile (version managed from 1.20.0)
[INFO] |  |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - omitted for duplicate)
[INFO] |  |  |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigdataoss:util:jar:1.4.5:compile
[INFO] |  |  +- (com.google.api-client:google-api-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client-jackson2:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.apis:google-api-services-storage:jar:v1-rev71-1.22.0:compile - version managed from v1-rev35-1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.oauth-client:google-oauth-client-java6:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-java:jar:3.0.0:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.1:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile
[INFO] |  +- com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:compile
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:compile - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- net.bytebuddy:byte-buddy:jar:1.4.3:compile
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  +- org.xerial.snappy:snappy-java:jar:1.1.2.1:compile
[INFO] |  +- org.apache.commons:commons-compress:jar:1.9:compile
[INFO] |  \- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] +- org.apache.beam:beam-sdks-java-io-google-cloud-platform:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:compile - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:compile
[INFO] |  |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile
[INFO] |  |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- com.google.http-client:google-http-client-jackson:jar:1.22.0:compile
[INFO] |  |  |  \- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile
[INFO] |  |  \- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- com.google.protobuf:protobuf-lite:jar:3.0.1:compile
[INFO] |  +- (io.grpc:grpc-core:jar:1.0.1:compile - omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- io.grpc:grpc-protobuf:jar:1.0.1:compile
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  |  +- com.google.protobuf:protobuf-java-util:jar:3.0.0:compile
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |  |  \- com.google.code.gson:gson:jar:2.3:compile
[INFO] |  |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  |  \- io.grpc:grpc-protobuf-lite:jar:1.0.1:compile
[INFO] |  |  |     +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  |     \- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- com.google.cloud.bigtable:bigtable-client-core:jar:0.9.2:compile
[INFO] |  |  +- (com.google.cloud.bigtable:bigtable-protos:jar:0.9.2:compile - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  |  +- commons-logging:commons-logging:jar:1.2:compile
[INFO] |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  +- com.google.auth:google-auth-library-appengine:jar:0.4.0:compile
[INFO] |  |  |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  |  \- com.google.appengine:appengine-api-1.0-sdk:jar:1.9.34:compile
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-handler:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  +- (io.netty:netty-transport:jar:4.1.3.Final:compile - omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-auth:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-core:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-netty:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (io.grpc:grpc-stub:jar:1.0.1:compile - version managed from 1.0.0; omitted for duplicate)
[INFO] |  |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  |  \- io.dropwizard.metrics:metrics-core:jar:3.1.2:compile
[INFO] |  |     \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:compile - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:compile - version managed from 2.6.1; omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 2.0.3; omitted for duplicate)
[INFO] |  +- io.netty:netty-tcnative-boringssl-static:jar:1.1.33.Fork18:runtime
[INFO] |  \- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] +- org.apache.beam:beam-examples-java:jar:0.4.0-incubating-SNAPSHOT:compile
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (org.apache.beam:beam-sdks-java-io-google-cloud-platform:jar:0.4.0-incubating-SNAPSHOT:compile - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:compile - omitted for duplicate)
[INFO] |  +- (com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:compile - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-proto-client:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- (com.google.cloud.datastore:datastore-v1-protos:jar:1.2.0:compile - omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:compile - omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] |  \- (org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] +- com.google.guava:guava:jar:19.0:compile
[INFO] +- org.slf4j:slf4j-api:jar:1.7.14:compile
[INFO] +- org.slf4j:slf4j-jdk14:jar:1.7.14:runtime
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.avro:avro:jar:1.8.1:compile
[INFO] |  +- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile
[INFO] |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:compile
[INFO] |  |  \- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:compile - omitted for duplicate)
[INFO] |  +- com.thoughtworks.paranamer:paranamer:jar:2.7:compile
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.1.3:compile - omitted for conflict with 1.1.2.1)
[INFO] |  +- (org.apache.commons:commons-compress:jar:1.8.1:compile - omitted for conflict with 1.9)
[INFO] |  +- org.tukaani:xz:jar:1.5:compile
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:compile - version managed from 1.7.7; omitted for duplicate)
[INFO] +- joda-time:joda-time:jar:2.4:compile
[INFO] +- org.hamcrest:hamcrest-all:jar:1.3:test
[INFO] +- org.mockito:mockito-all:jar:1.9.5:test
[INFO] +- junit:junit:jar:4.11:test
[INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] +- com.google.apis:google-api-services-bigquery:jar:v2-rev295-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.http-client:google-http-client:jar:1.22.0:compile
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.httpcomponents:httpclient:jar:4.0.1:compile
[INFO] |     +- org.apache.httpcomponents:httpcore:jar:4.0.1:compile
[INFO] |     +- (commons-logging:commons-logging:jar:1.1.1:compile - omitted for conflict with 1.2)
[INFO] |     \- commons-codec:commons-codec:jar:1.3:compile
[INFO] +- com.google.oauth-client:google-oauth-client:jar:1.22.0:compile
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:compile - version managed from 1.3.9; omitted for duplicate)
[INFO] +- com.google.apis:google-api-services-pubsub:jar:v1-rev10-1.22.0:compile
[INFO] |  \- (com.google.api-client:google-api-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] +- com.google.api-client:google-api-client:jar:1.22.0:compile
[INFO] |  +- (com.google.oauth-client:google-oauth-client:jar:1.22.0:compile - version managed from 1.20.0; omitted for duplicate)
[INFO] |  \- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:compile - version managed from 1.19.0; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-direct-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-flink_2.10:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- org.apache.flink:flink-streaming-java_2.10:jar:1.1.2:runtime
[INFO] |  |  +- org.apache.flink:flink-core:jar:1.1.2:runtime
[INFO] |  |  |  +- org.apache.flink:flink-annotations:jar:1.1.2:runtime
[INFO] |  |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- org.apache.flink:flink-metrics-core:jar:1.1.2:runtime
[INFO] |  |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.esotericsoftware.kryo:kryo:jar:2.24.0:runtime - omitted for conflict with 2.21)
[INFO] |  |  |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- org.apache.flink:flink-runtime_2.10:jar:1.1.2:runtime
[INFO] |  |  |  +- (org.apache.flink:flink-core:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.flink:flink-java:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-cli:commons-cli:jar:1.3.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (io.netty:netty-all:jar:4.0.27.Final:runtime - omitted for conflict with 4.0.29.Final)
[INFO] |  |  |  +- org.javassist:javassist:jar:3.18.2-GA:runtime
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.7:runtime - omitted for conflict with 2.3.11)
[INFO] |  |  |  +- (com.typesafe.akka:akka-remote_2.10:jar:2.3.7:runtime - omitted for conflict with 2.3.11)
[INFO] |  |  |  +- (com.typesafe.akka:akka-slf4j_2.10:jar:2.3.7:runtime - omitted for conflict with 2.3.11)
[INFO] |  |  |  +- org.clapper:grizzled-slf4j_2.10:jar:1.0.2:runtime
[INFO] |  |  |  |  +- (org.scala-lang:scala-library:jar:2.10.3:runtime - omitted for conflict with 2.10.4)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- com.github.scopt:scopt_2.10:jar:3.2.0:runtime
[INFO] |  |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.0:runtime - omitted for conflict with 3.1.2)
[INFO] |  |  |  +- (io.dropwizard.metrics:metrics-jvm:jar:3.1.0:runtime - omitted for conflict with 3.1.2)
[INFO] |  |  |  +- (io.dropwizard.metrics:metrics-json:jar:3.1.0:runtime - omitted for conflict with 3.1.2)
[INFO] |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:runtime - omitted for conflict with 3.4.5)
[INFO] |  |  |  +- (com.twitter:chill_2.10:jar:0.7.4:runtime - omitted for conflict with 0.5.0)
[INFO] |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-clients_2.10:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-math3:jar:3.5:runtime - omitted for conflict with 3.4.1)
[INFO] |  |  +- org.apache.sling:org.apache.sling.commons.json:jar:2.0.6:runtime
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for conflict with 1.7.10)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  \- org.apache.flink:force-shading:jar:1.1.2:runtime
[INFO] |  +- org.apache.flink:flink-java:jar:1.1.2:runtime
[INFO] |  |  +- (org.apache.flink:flink-core:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- org.apache.flink:flink-shaded-hadoop2:jar:1.1.2:runtime
[INFO] |  |  |  +- (commons-cli:commons-cli:jar:1.3.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-math3:jar:3.5:runtime - omitted for duplicate)
[INFO] |  |  |  +- xmlenc:xmlenc:jar:0.52:runtime
[INFO] |  |  |  +- (commons-codec:commons-codec:jar:1.4:runtime - omitted for conflict with 1.3)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.4:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-net:commons-net:jar:3.1:runtime - omitted for conflict with 2.2)
[INFO] |  |  |  +- commons-collections:commons-collections:jar:3.2.1:runtime
[INFO] |  |  |  +- javax.servlet:servlet-api:jar:2.5:runtime
[INFO] |  |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:runtime
[INFO] |  |  |  +- (com.sun.jersey:jersey-core:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  +- commons-el:commons-el:jar:1.0:runtime
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.0.3:runtime - omitted for conflict with 1.2)
[INFO] |  |  |  +- (commons-logging:commons-logging:jar:1.1.3:runtime - omitted for conflict with 1.2)
[INFO] |  |  |  +- com.jamesmurty.utils:java-xmlbuilder:jar:0.4:runtime
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.6:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  +- commons-configuration:commons-configuration:jar:1.7:runtime
[INFO] |  |  |  |  +- (commons-collections:commons-collections:jar:3.2.1:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (commons-lang:commons-lang:jar:2.6:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (commons-logging:commons-logging:jar:1.1.1:runtime - omitted for conflict with 1.2)
[INFO] |  |  |  |  \- (commons-digester:commons-digester:jar:1.8.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- commons-digester:commons-digester:jar:1.8.1:runtime
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.1.1:runtime - omitted for conflict with 1.2)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:runtime - omitted for conflict with 1.9.13)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.8.8:runtime - omitted for conflict with 1.9.13)
[INFO] |  |  |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  |  +- (com.thoughtworks.paranamer:paranamer:jar:2.3:runtime - omitted for conflict with 2.7)
[INFO] |  |  |  +- (org.xerial.snappy:snappy-java:jar:1.0.5:runtime - omitted for conflict with 1.1.2.1)
[INFO] |  |  |  +- com.jcraft:jsch:jar:0.1.42:runtime
[INFO] |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.6:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-compress:jar:1.4.1:runtime - omitted for conflict with 1.9)
[INFO] |  |  |  +- (org.tukaani:xz:jar:1.0:runtime - omitted for conflict with 1.5)
[INFO] |  |  |  +- commons-beanutils:commons-beanutils-bean-collections:jar:1.8.3:runtime
[INFO] |  |  |  |  \- (commons-logging:commons-logging:jar:1.1.1:runtime - omitted for conflict with 1.2)
[INFO] |  |  |  +- commons-daemon:commons-daemon:jar:1.0.13:runtime
[INFO] |  |  |  +- javax.xml.bind:jaxb-api:jar:2.2.2:runtime
[INFO] |  |  |  |  +- (javax.xml.stream:stax-api:jar:1.0-2:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (javax.activation:activation:jar:1.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- javax.xml.stream:stax-api:jar:1.0-2:runtime
[INFO] |  |  |  +- javax.activation:activation:jar:1.1:runtime
[INFO] |  |  |  +- com.google.inject:guice:jar:3.0:runtime
[INFO] |  |  |  |  +- (javax.inject:javax.inject:jar:1:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (aopalliance:aopalliance:jar:1.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- javax.inject:javax.inject:jar:1:runtime
[INFO] |  |  |  +- aopalliance:aopalliance:jar:1.0:runtime
[INFO] |  |  |  +- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  \- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-math3:jar:3.5:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  +- org.apache.flink:flink-clients_2.10:jar:1.1.2:runtime
[INFO] |  |  +- (org.apache.flink:flink-core:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- org.apache.flink:flink-optimizer_2.10:jar:1.1.2:runtime
[INFO] |  |  |  +- (org.apache.flink:flink-core:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.flink:flink-runtime_2.10:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.flink:flink-java:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.flink:flink-java:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- commons-cli:commons-cli:jar:1.3.1:runtime
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.3.2:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.7:runtime - omitted for duplicate)
[INFO] |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  \- (org.apache.flink:force-shading:jar:1.1.2:runtime - omitted for duplicate)
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  \- (org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-google-cloud-dataflow-java:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-jackson2:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client-protobuf:jar:1.22.0:compile - version managed from 1.20.0; scope updated from runtime; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-dataflow:jar:v1b3-rev43-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- com.google.apis:google-api-services-clouddebugger:jar:v2-rev8-1.22.0:runtime
[INFO] |  |  \- (com.google.api-client:google-api-client:jar:1.22.0:runtime - version managed from 1.20.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-credentials:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.auth:google-auth-library-oauth2-http:jar:0.6.0:runtime - version managed from 0.4.0; omitted for duplicate)
[INFO] |  +- (com.google.cloud.bigdataoss:util:jar:1.4.5:runtime - omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  +- (com.google.protobuf:protobuf-lite:jar:3.0.1:runtime - omitted for duplicate)
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.2; omitted for duplicate)
[INFO] |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] +- org.apache.beam:beam-runners-spark:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  +- de.javakaffee:kryo-serializers:jar:0.39:runtime
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.2; omitted for duplicate)
[INFO] |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  +- (com.google.http-client:google-http-client:jar:1.22.0:runtime - version managed from 1.19.0; omitted for duplicate)
[INFO] |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- (org.apache.beam:beam-runners-core-java:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  +- org.apache.avro:avro-mapred:jar:hadoop2:1.8.1:runtime
[INFO] |  |  +- org.apache.avro:avro-ipc:jar:1.8.1:runtime
[INFO] |  |  |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  |  +- org.mortbay.jetty:jetty:jar:6.1.26:runtime
[INFO] |  |  |  |  \- (org.mortbay.jetty:jetty-util:jar:6.1.26:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:runtime - omitted for duplicate)
[INFO] |  |  |  +- (io.netty:netty:jar:3.5.13.Final:runtime - omitted for conflict with 3.8.0.Final)
[INFO] |  |  |  +- org.apache.velocity:velocity:jar:1.7:runtime
[INFO] |  |  |  |  +- (commons-collections:commons-collections:jar:3.2.1:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (commons-lang:commons-lang:jar:2.4:runtime - omitted for conflict with 2.6)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  +- (org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:runtime - omitted for duplicate)
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.9:runtime - omitted for conflict with 1.3)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- org.apache.beam:beam-sdks-java-io-kafka:jar:0.4.0-incubating-SNAPSHOT:runtime
[INFO] |  |  +- (org.apache.beam:beam-sdks-java-core:jar:0.4.0-incubating-SNAPSHOT:runtime - omitted for duplicate)
[INFO] |  |  +- (org.apache.kafka:kafka-clients:jar:0.9.0.1:runtime - omitted for duplicate)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |  |  +- (joda-time:joda-time:jar:2.4:runtime - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.7.0; omitted for duplicate)
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 18.0; omitted for duplicate)
[INFO] |  |  \- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  \- org.apache.kafka:kafka-clients:jar:0.9.0.1:runtime
[INFO] |     +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.6; omitted for duplicate)
[INFO] |     +- (org.xerial.snappy:snappy-java:jar:1.1.1.7:runtime - omitted for conflict with 1.1.2.1)
[INFO] |     \- (net.jpountz.lz4:lz4:jar:1.2.0:runtime - omitted for conflict with 1.3.0)
[INFO] +- org.apache.spark:spark-core_2.10:jar:1.6.2:runtime
[INFO] |  +- (org.apache.avro:avro-mapred:jar:hadoop2:1.7.7:runtime - omitted for conflict with 1.8.1)
[INFO] |  +- com.twitter:chill_2.10:jar:0.5.0:runtime
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  +- (com.twitter:chill-java:jar:0.5.0:runtime - omitted for duplicate)
[INFO] |  |  \- com.esotericsoftware.kryo:kryo:jar:2.21:runtime
[INFO] |  |     +- com.esotericsoftware.reflectasm:reflectasm:jar:shaded:1.07:runtime
[INFO] |  |     +- com.esotericsoftware.minlog:minlog:jar:1.2:runtime
[INFO] |  |     \- org.objenesis:objenesis:jar:1.2:runtime
[INFO] |  +- com.twitter:chill-java:jar:0.5.0:runtime
[INFO] |  |  \- (com.esotericsoftware.kryo:kryo:jar:2.21:runtime - omitted for duplicate)
[INFO] |  +- org.apache.xbean:xbean-asm5-shaded:jar:4.4:runtime
[INFO] |  +- org.apache.hadoop:hadoop-client:jar:2.2.0:runtime
[INFO] |  |  +- org.apache.hadoop:hadoop-common:jar:2.2.0:runtime
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-annotations:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- (commons-cli:commons-cli:jar:1.2:runtime - omitted for conflict with 1.3.1)
[INFO] |  |  |  +- org.apache.commons:commons-math:jar:2.1:runtime
[INFO] |  |  |  +- (xmlenc:xmlenc:jar:0.52:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-httpclient:commons-httpclient:jar:3.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-codec:commons-codec:jar:1.4:runtime - omitted for conflict with 1.3)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  +- (commons-net:commons-net:jar:3.1:runtime - omitted for duplicate)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.5:runtime - omitted for conflict with 2.6)
[INFO] |  |  |  +- (commons-configuration:commons-configuration:jar:1.6:runtime - omitted for conflict with 1.7)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:runtime - omitted for conflict with 1.9.13)
[INFO] |  |  |  +- (org.apache.avro:avro:jar:1.8.1:runtime - version managed from 1.7.4; omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-auth:jar:2.2.0:runtime
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  +- (commons-codec:commons-codec:jar:1.4:runtime - omitted for conflict with 1.3)
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:runtime - omitted for conflict with 3.4.6)
[INFO] |  |  |  \- (org.apache.commons:commons-compress:jar:1.4.1:runtime - omitted for conflict with 1.9)
[INFO] |  |  +- org.apache.hadoop:hadoop-hdfs:jar:2.2.0:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 11.0.2; omitted for duplicate)
[INFO] |  |  |  +- (org.mortbay.jetty:jetty-util:jar:6.1.26:runtime - omitted for duplicate)
[INFO] |  |  |  +- (commons-cli:commons-cli:jar:1.2:runtime - omitted for conflict with 1.3.1)
[INFO] |  |  |  +- (commons-codec:commons-codec:jar:1.4:runtime - omitted for conflict with 1.3)
[INFO] |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  +- (commons-lang:commons-lang:jar:2.5:runtime - omitted for conflict with 2.6)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.codehaus.jackson:jackson-core-asl:jar:1.8.8:runtime - omitted for conflict with 1.9.13)
[INFO] |  |  |  \- (xmlenc:xmlenc:jar:0.52:runtime - omitted for duplicate)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-app:jar:2.2.0:runtime
[INFO] |  |  |  +- org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.2.0:runtime
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- org.apache.hadoop:hadoop-yarn-client:jar:2.2.0:runtime
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  |  |  +- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- org.apache.hadoop:hadoop-yarn-server-common:jar:2.2.0:runtime
[INFO] |  |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:runtime - omitted for conflict with 3.4.6)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  |  +- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  |  |  +- (com.google.inject:guice:jar:3.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey.jersey-test-framework:jersey-test-framework-grizzly2:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-server:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  +- (com.sun.jersey:jersey-json:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  |  \- (com.sun.jersey.contribs:jersey-guice:jar:1.9:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.2.0:runtime
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:runtime
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  \- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.2.0:runtime
[INFO] |  |  |  +- org.apache.hadoop:hadoop-yarn-common:jar:2.2.0:runtime
[INFO] |  |  |  |  +- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.hadoop:hadoop-yarn-api:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  |  \- (commons-io:commons-io:jar:2.1:runtime - omitted for conflict with 2.4)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  +- org.apache.hadoop:hadoop-mapreduce-client-jobclient:jar:2.2.0:runtime
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.2.0:runtime - omitted for duplicate)
[INFO] |  |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-log4j12:jar:1.7.5:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  \- org.apache.hadoop:hadoop-annotations:jar:2.2.0:runtime
[INFO] |  +- org.apache.spark:spark-launcher_2.10:jar:1.6.2:runtime
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:runtime - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-network-common_2.10:jar:1.6.2:runtime
[INFO] |  |  +- (io.netty:netty-all:jar:4.0.29.Final:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:runtime - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-network-shuffle_2.10:jar:1.6.2:runtime
[INFO] |  |  +- (org.apache.spark:spark-network-common_2.10:jar:1.6.2:runtime - omitted for duplicate)
[INFO] |  |  +- org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:runtime
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:runtime - omitted for duplicate)
[INFO] |  +- org.apache.spark:spark-unsafe_2.10:jar:1.6.2:runtime
[INFO] |  |  +- (com.twitter:chill_2.10:jar:0.5.0:runtime - omitted for duplicate)
[INFO] |  |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  |  \- (org.spark-project.spark:unused:jar:1.0.0:runtime - omitted for duplicate)
[INFO] |  +- net.java.dev.jets3t:jets3t:jar:0.7.1:runtime
[INFO] |  |  +- (commons-codec:commons-codec:jar:1.3:runtime - omitted for duplicate)
[INFO] |  |  \- commons-httpclient:commons-httpclient:jar:3.1:runtime
[INFO] |  |     \- (commons-codec:commons-codec:jar:1.2:runtime - omitted for conflict with 1.3)
[INFO] |  +- org.apache.curator:curator-recipes:jar:2.4.0:runtime
[INFO] |  |  +- org.apache.curator:curator-framework:jar:2.4.0:runtime
[INFO] |  |  |  +- org.apache.curator:curator-client:jar:2.4.0:runtime
[INFO] |  |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.6.4; omitted for duplicate)
[INFO] |  |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:runtime - omitted for conflict with 3.4.6)
[INFO] |  |  |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.zookeeper:zookeeper:jar:3.4.5:runtime - omitted for conflict with 3.4.6)
[INFO] |  |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  +- org.apache.zookeeper:zookeeper:jar:3.4.5:runtime
[INFO] |  |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.6.1; omitted for duplicate)
[INFO] |  |  |  +- (org.slf4j:slf4j-log4j12:jar:1.6.1:runtime - omitted for conflict with 1.7.7)
[INFO] |  |  |  +- (log4j:log4j:jar:1.2.15:runtime - omitted for conflict with 1.2.17)
[INFO] |  |  |  \- jline:jline:jar:0.9.94:runtime
[INFO] |  |  \- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  +- org.eclipse.jetty.orbit:javax.servlet:jar:3.0.0.v201112011016:runtime
[INFO] |  +- org.apache.commons:commons-lang3:jar:3.3.2:runtime
[INFO] |  +- org.apache.commons:commons-math3:jar:3.4.1:runtime
[INFO] |  +- (com.google.code.findbugs:jsr305:jar:3.0.1:runtime - version managed from 1.3.9; omitted for duplicate)
[INFO] |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- org.slf4j:jcl-over-slf4j:jar:1.7.10:runtime
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.10; omitted for duplicate)
[INFO] |  +- log4j:log4j:jar:1.2.17:runtime
[INFO] |  +- org.slf4j:slf4j-log4j12:jar:1.7.10:runtime
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.10; omitted for duplicate)
[INFO] |  |  \- (log4j:log4j:jar:1.2.17:runtime - omitted for duplicate)
[INFO] |  +- com.ning:compress-lzf:jar:1.0.3:runtime
[INFO] |  +- (org.xerial.snappy:snappy-java:jar:1.1.2.1:runtime - omitted for duplicate)
[INFO] |  +- net.jpountz.lz4:lz4:jar:1.3.0:runtime
[INFO] |  +- org.roaringbitmap:RoaringBitmap:jar:0.5.11:runtime
[INFO] |  +- commons-net:commons-net:jar:2.2:runtime
[INFO] |  +- com.typesafe.akka:akka-remote_2.10:jar:2.3.11:runtime
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:runtime - omitted for duplicate)
[INFO] |  |  +- com.typesafe.akka:akka-actor_2.10:jar:2.3.11:runtime
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.4:runtime - omitted for duplicate)
[INFO] |  |  |  \- com.typesafe:config:jar:1.2.1:runtime
[INFO] |  |  +- io.netty:netty:jar:3.8.0.Final:runtime
[INFO] |  |  +- (com.google.protobuf:protobuf-java:jar:3.0.0:runtime - version managed from 2.5.0; omitted for duplicate)
[INFO] |  |  \- org.uncommons.maths:uncommons-maths:jar:1.2.2a:runtime
[INFO] |  +- com.typesafe.akka:akka-slf4j_2.10:jar:2.3.11:runtime
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.4:runtime - omitted for duplicate)
[INFO] |  |  +- (com.typesafe.akka:akka-actor_2.10:jar:2.3.11:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.5; omitted for duplicate)
[INFO] |  +- org.scala-lang:scala-library:jar:2.10.5:runtime
[INFO] |  +- org.json4s:json4s-jackson_2.10:jar:3.2.10:runtime
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.0:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  +- org.json4s:json4s-core_2.10:jar:3.2.10:runtime
[INFO] |  |  |  +- (org.scala-lang:scala-library:jar:2.10.0:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  |  +- org.json4s:json4s-ast_2.10:jar:3.2.10:runtime
[INFO] |  |  |  |  \- (org.scala-lang:scala-library:jar:2.10.0:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  |  +- (com.thoughtworks.paranamer:paranamer:jar:2.6:runtime - omitted for conflict with 2.7)
[INFO] |  |  |  \- org.scala-lang:scalap:jar:2.10.0:runtime
[INFO] |  |  |     \- org.scala-lang:scala-compiler:jar:2.10.0:runtime
[INFO] |  |  |        +- (org.scala-lang:scala-library:jar:2.10.0:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  |        \- (org.scala-lang:scala-reflect:jar:2.10.0:runtime - omitted for conflict with 2.10.6)
[INFO] |  |  \- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.3.1; omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-server:jar:1.9:runtime
[INFO] |  |  +- asm:asm:jar:3.1:runtime
[INFO] |  |  \- (com.sun.jersey:jersey-core:jar:1.9:runtime - omitted for duplicate)
[INFO] |  +- com.sun.jersey:jersey-core:jar:1.9:runtime
[INFO] |  +- org.apache.mesos:mesos:jar:shaded-protobuf:0.21.1:runtime
[INFO] |  +- io.netty:netty-all:jar:4.0.29.Final:runtime
[INFO] |  +- com.clearspring.analytics:stream:jar:2.7.0:runtime
[INFO] |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:compile - scope updated from runtime; omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-jvm:jar:3.1.2:runtime
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-json:jar:3.1.2:runtime
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:runtime - omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.2; omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- io.dropwizard.metrics:metrics-graphite:jar:3.1.2:runtime
[INFO] |  |  +- (io.dropwizard.metrics:metrics-core:jar:3.1.2:runtime - omitted for duplicate)
[INFO] |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.7; omitted for duplicate)
[INFO] |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  +- com.fasterxml.jackson.module:jackson-module-scala_2.10:jar:2.7.2:runtime (version managed from 2.4.4)
[INFO] |  |  +- (org.scala-lang:scala-library:jar:2.10.6:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  +- org.scala-lang:scala-reflect:jar:2.10.6:runtime
[INFO] |  |  |  \- (org.scala-lang:scala-library:jar:2.10.6:runtime - omitted for conflict with 2.10.5)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-core:jar:2.7.2:runtime - version managed from 2.1.3; omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-annotations:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |  \- com.fasterxml.jackson.module:jackson-module-paranamer:jar:2.7.2:runtime
[INFO] |  |     +- (com.fasterxml.jackson.core:jackson-databind:jar:2.7.2:runtime - version managed from 2.4.4; omitted for duplicate)
[INFO] |  |     \- (com.thoughtworks.paranamer:paranamer:jar:2.8:runtime - omitted for conflict with 2.7)
[INFO] |  +- org.apache.ivy:ivy:jar:2.4.0:runtime
[INFO] |  +- oro:oro:jar:2.0.8:runtime
[INFO] |  +- org.tachyonproject:tachyon-client:jar:0.8.2:runtime
[INFO] |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  +- commons-lang:commons-lang:jar:2.4:runtime
[INFO] |  |  +- commons-io:commons-io:jar:2.4:runtime
[INFO] |  |  +- (org.apache.commons:commons-lang3:jar:3.0:runtime - omitted for conflict with 3.3.2)
[INFO] |  |  +- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  +- org.tachyonproject:tachyon-underfs-hdfs:jar:0.8.2:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  +- (org.apache.commons:commons-lang3:jar:3.0:runtime - omitted for conflict with 3.3.2)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  +- org.tachyonproject:tachyon-underfs-s3:jar:0.8.2:runtime
[INFO] |  |  |  +- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |  |  \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.2; omitted for duplicate)
[INFO] |  |  \- org.tachyonproject:tachyon-underfs-local:jar:0.8.2:runtime
[INFO] |  |     +- (com.google.guava:guava:jar:19.0:runtime - version managed from 14.0.1; omitted for duplicate)
[INFO] |  |     \- (org.slf4j:slf4j-api:jar:1.7.14:runtime - version managed from 1.7.2; omitted for duplicate)
[INFO] |  +- net.razorvine:pyrolite:jar:4.9:runtime
[INFO] |  +- net.sf.py4j:py4j:jar:0.9:runtime
[INFO] |  \- org.spark-project.spark:unused:jar:1.0.0:runtime
[INFO] \- org.apache.spark:spark-streaming_2.10:jar:1.6.2:runtime
[INFO]    +- (org.apache.spark:spark-core_2.10:jar:1.6.2:runtime - omitted for duplicate)
[INFO]    +- (org.scala-lang:scala-library:jar:2.10.5:runtime - omitted for duplicate)
[INFO]    \- (org.spark-project.spark:unused:jar:1.0.0:runtime - omitted for duplicate)
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Apache Beam :: Parent ............................. SUCCESS [0.821s]
[INFO] Apache Beam :: SDKs :: Java :: Build Tools ........ SUCCESS [0.008s]
[INFO] Apache Beam :: SDKs ............................... SUCCESS [0.007s]
[INFO] Apache Beam :: SDKs :: Java ....................... SUCCESS [0.007s]
[INFO] Apache Beam :: SDKs :: Java :: Core ............... SUCCESS [1.124s]
[INFO] Apache Beam :: Runners ............................ SUCCESS [0.013s]
[INFO] Apache Beam :: Runners :: Core Java ............... SUCCESS [0.501s]
[INFO] Apache Beam :: Runners :: Direct Java ............. SUCCESS [0.284s]
[INFO] Apache Beam :: Runners :: Google Cloud Dataflow ... SUCCESS [0.119s]
[INFO] Apache Beam :: SDKs :: Java :: IO ................. SUCCESS [0.004s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: Google Cloud Platform  SUCCESS [0.383s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: HDFS ......... SUCCESS [0.912s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: JMS .......... SUCCESS [0.118s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: Kafka ........ SUCCESS [0.052s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: Kinesis ...... SUCCESS [0.122s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: MongoDB ...... SUCCESS [0.137s]
[INFO] Apache Beam :: SDKs :: Java :: IO :: JDBC ......... SUCCESS [0.131s]
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes ... SUCCESS [0.003s]
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes :: Starter  SUCCESS [0.441s]
[INFO] Apache Beam :: SDKs :: Java :: Maven Archetypes :: Examples  SUCCESS [0.189s]
[INFO] Apache Beam :: SDKs :: Java :: Extensions ......... SUCCESS [0.003s]
[INFO] Apache Beam :: SDKs :: Java :: Extensions :: Join library  SUCCESS [0.037s]
[INFO] Apache Beam :: SDKs :: Java :: Extensions :: Sorter  SUCCESS [0.276s]
[INFO] Apache Beam :: SDKs :: Java :: Java 8 Tests ....... SUCCESS [0.176s]
[INFO] Apache Beam :: Runners :: Flink ................... SUCCESS [0.002s]
[INFO] Apache Beam :: Runners :: Flink :: Core ........... SUCCESS [0.679s]
[INFO] Apache Beam :: Runners :: Flink :: Examples ....... SUCCESS [0.114s]
[INFO] Apache Beam :: Runners :: Spark ................... SUCCESS [1.003s]
[INFO] Apache Beam :: Runners :: Apex .................... SUCCESS [0.583s]
[INFO] Apache Beam :: Examples ........................... SUCCESS [0.002s]
[INFO] Apache Beam :: Examples :: Java ................... SUCCESS [0.049s]
[INFO] Apache Beam :: Examples :: Java 8 ................. SUCCESS [0.691s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 9.949s
[INFO] Finished at: Mon Nov 21 14:42:38 PST 2016
[INFO] Final Memory: 46M/485M
[INFO] ------------------------------------------------------------------------
```
