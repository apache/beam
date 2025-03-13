---
title: "Python SDK Roadmap"
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

# Python SDK Roadmap

## Sunsetting Python 2 Support

The Apache Beam community voted to join [the pledge](https://python3statement.org/) to sunset Python 2 support in 2020. The Beam community will discontinue Python 2 support in 2020 and cannot guarantee long-term functional support or maintenance of Beam on Python 2. To ensure minimal disruption to your service, we strongly recommend that you upgrade to Python 3 as soon as possible.

## Python 3 Support

Apache Beam 2.14.0 and higher support Python 3.5, 3.6, and 3.7. We're continuing to [improve](https://issues.apache.org/jira/browse/BEAM-1251?focusedCommentId=16890504&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-1689050) the experience for Python 3 users and add support for Python 3.x minor versions ([BEAM-8494](https://issues.apache.org/jira/browse/BEAM-8494)):


 - [Kanban Board](https://issues.apache.org/jira/secure/RapidBoard.jspa?rapidView=245&view=detail)
 - [Python 3 Conversion Quick Start Guide](https://docs.google.com/document/d/1s1BJVCY65LB_SYK1SU1u7NbZiFANoq-nEYaEvzRbYlA)
 - [Tracking Issue](https://issues.apache.org/jira/browse/BEAM-1251)
 - [Original Proposal](https://docs.google.com/document/d/1xDG0MWVlDKDPu_IW9gtMvxi2S9I0GB0VDTkPhjXT0nE)

Contributions and feedback are welcome!

If you are interested in helping, you can select an unassigned issue on the Kanban board and assign it to yourself. If you cannot assign the issue to yourself, comment on the issue. When submitting a new PR, please tag [@aaltay](https://github.com/aaltay), and [@tvalentyn](https://github.com/tvalentyn).

To report a Python 3 related issue, create a subtask in [BEAM-1251](https://issues.apache.org/jira/browse/BEAM-1251) and cc: [~altay] and [~tvalentyn] in a JIRA comment. The best way to help us identify and investigate the issue is with a minimal pipeline that reproduces the issue.

You can also discuss encountered issues on user@ or dev@ mailing lists as appropriate.
