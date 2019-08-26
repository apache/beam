---
layout: section
title: "Beam Release Guide"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/
redirect_from:
 - /release-guide/
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

# Apache Beam Release Guide

*****

### Introduction

The Apache Beam project periodically declares and publishes releases. A release is one or more packages of the project artifact(s) that are approved for general public distribution and use. They may come with various degrees of caveat regarding their perceived quality and potential for change, such as “alpha”, “beta”, “incubating”, “stable”, etc.

The Beam community treats releases with great importance. They are a public face of the project and most users interact with the project only through the releases. Releases are signed off by the entire Beam community in a public vote.

Each release is executed by a *Release Manager*, who is selected among the Beam committers. This document describes the process that the Release Manager follows to perform a release. Any changes to this process should be discussed and adopted on the [dev@ mailing list]({{ site.baseurl }}/get-started/support/).

Please remember that publishing software has legal consequences. This guide complements the foundation-wide [Product Release Policy](http://www.apache.org/dev/release.html) and [Release Distribution Policy](http://www.apache.org/dev/release-distribution).

### Overview

![Alt text]({{ "/images/release-guide-1.png" | prepend: site.baseurl }} "Release Process"){:width="100%"}

The release process consists of several steps:

1. Decide to release
1. Prepare for the release
1. Build a release candidate
1. Vote on the release candidate
1. During vote process, run validation tests
1. If necessary, fix any issues and go back to step 3.
1. Finalize the release
1. Promote the release

**********

### Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based decision of the entire community.

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any objections should be resolved by consensus before starting the release.

If you're not a committer you might not be able to perform most of the steps.

Some steps require PMC permissions so you will need to reach out to get help if you're not a PMC member.

### Checklist to proceed to the next step

1. Community agrees to release
1. Community selects a Release Manager

**********

<a class="button button--primary" href="{{'/contribute/release-guide/prepare/'|prepend:site.baseurl}}">Next Step: Prepare for Release</a>
