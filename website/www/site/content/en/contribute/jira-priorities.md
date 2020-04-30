---
layout: section
title: "Jira Priorities"
permalink: /contribute/jira-priorities/
section_menu: section-menu/contribute.html
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

# Jira Priorities

## Blocker / P0

*Expectation*: Drop everything else and work continuously to resolve. Note that
the term "blocker" does not refer to blocking releases. A P0 issue is more
urgent than simply blocking the next release.

*Example Blocker/P0 issues*:

 - the build is broken, halting all development
 - the website is down
 - a vulnerability requires a point release ASAP

## Critical / P1

*Expectation*: Continuous status updates. Critical bugs should not be
unassigned. Most critical bugs should block release.

*Example Critical/P1 issues*:

 - data loss error
 - important component is nonfunctional for important use cases
 - major performance regression
 - failing postcommit test
 - flaky test

## Major / P2

*Expectation*: Most tickets fall into this priority. These can be planned and
executed by anyone who is interested. No special urgency is associated.

*Example Major/P2 issues*

 - typical feature request
 - bug that affects some use cases but don't make a component nonfunctional
 - ignored ("sickbayed") test

## Minor / P3

*Expectation*: Nice-to-have improvements.

*Example Minor/P3 issues*

 - feature request that is nice-to-have

## Trivial / P4

*Expectation*: Nice-to-have improvements that are also very small and easy.
Usually it is quicker to just fix them than to file a bug, but the Jira
can be referenced by a pull request and shows up in release notes.

*Example Trivial/P4 issues*

 - spelling errors in comments or code

