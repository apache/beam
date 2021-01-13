---
title: "Jira Priorities"
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

## P0: Outage

*Expectation*: Drop everything else and work continuously to resolve. An outage
means that some piece of infrastructure that the community relies on is down. A
P0 issue is _more_ urgent than simply blocking the next release.

*Example P0 issues*:

 - the build is broken, halting all development
 - the website is down
 - a vulnerability requires a point release ASAP

## P1: Critical

*Expectation*: Continuous status updates. P1 bugs should not be
unassigned. Most P1 bugs should block release.

*Example P1 issues*:

 - data loss error
 - important component is nonfunctional for important use cases
 - major performance regression
 - failing postcommit test
 - flaky test

## P2: Default

*Expectation*: Most tickets fall into this priority. These can be planned and
executed by anyone who is interested. No special urgency is associated, but if
no action is taken on a P2 ticket for a long time, it indicates it is actually
just P3/nice-to-have.

*Example P2 issues*

 - typical feature request
 - bug that affects some use cases but don't make a component nonfunctional
 - ignored ("sickbayed") test

## P3: Nice-to-have

*Expectation*: Nice-to-have improvements.

*Example P3 issues*

 - feature request that is nice-to-have
 - ticket filed as P2 that no one finds time to work on

## P4

*Expectation*: Nice-to-have improvements that are also very small and easy.
Usually it is quicker to just fix them than to file a bug, but the Jira
can be referenced by a pull request and shows up in release notes.

*Example P4 issues*

 - spelling errors in comments or code

