---
layout: section
title: "Beam Release Guide: 07 - Validate RC"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/validate-candidate/
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

# Validate the RC and vote

*****

## Vote on the release candidate

Once you have built and individually reviewed the release candidate, please share it for the community-wide review.
Please review foundation-wide [voting guidelines](http://www.apache.org/foundation/voting.html) for more information.

Start the review-and-vote thread on the dev@ mailing list. Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: dev@beam.apache.org
Subject: [VOTE] Release 1.2.3, release candidate #3

Hi everyone,
Please review and vote on the release candidate #3 for the version 1.2.3, as follows:
[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)


The complete staging area is available for your review, which includes:
* JIRA release notes [1],
* the official Apache source release to be deployed to dist.apache.org [2], which is signed with the key with fingerprint FFFFFFFF [3],
* all artifacts to be deployed to the Maven Central Repository [4],
* source code tag "v1.2.3-RC3" [5],
* website pull request listing the release [6], publishing the API reference manual [7], and the blog post [8].
* Java artifacts were built with Maven MAVEN_VERSION and OpenJDK/Oracle JDK JDK_VERSION.
* Python artifacts are deployed along with the source release to the dist.apache.org [2].
* Validation sheet with a tab for 1.2.3 release to help with validation [9].

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager

[1] https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=...
[2] https://dist.apache.org/repos/dist/dev/beam/1.2.3/
[3] https://dist.apache.org/repos/dist/release/beam/KEYS
[4] https://repository.apache.org/content/repositories/orgapachebeam-NNNN/
[5] https://github.com/apache/beam/tree/v1.2.3-RC3
[6] https://github.com/apache/beam/pull/...
[7] https://github.com/apache/beam-site/pull/...
[8] https://github.com/apache/beam/pull/...
[9] https://docs.google.com/spreadsheets/d/1qk-N5vjXvbcEk68GjbkSZTR8AGqyNUM-oLFo_ZXBpJw/edit#gid=...
```

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote.
There’s no need to wait 72 hours. Proceed to the `Fix Issues` step below and address the problem. However, some issues don’t require cancellation.
For example, if an issue is found in the website pull request, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email.
Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: dev@beam.apache.org
Subject: [RESULT] [VOTE] Release 1.2.3, release candidate #3

I'm happy to announce that we have unanimously approved this release.

There are XXX approving votes, XXX of which are binding:
* approver 1
* approver 2
* approver 3
* approver 4

There are no disapproving votes.

Thanks everyone!
```


*****


## Run validation tests

All tests listed in this [spreadsheet](https://s.apache.org/beam-release-validation)

Since there are a bunch of tests, we recommend you running validations using automation script.
In case of script failure, you can still run all of them manually.

### Run validations using `run_rc_validation.sh`

* Script: [run_rc_validation.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/run_rc_validation.sh)

* Usage
  1. First update [script.config](https://github.com/apache/beam/blob/master/release/src/main/scripts/script.config) with correct config value (e.g. release version, rc number).
  1. Then run
     ```sh
     ./beam/release/src/main/scripts/run_rc_validation.sh
     ```

The script runs quickstarts and mobile games examples on multiple SDKs and runners.

* Tasks you need to do manually
  1. Check whether validations succeed by following console output instructions.
  1. Terminate streaming jobs and java injector.
  1. Sign up [spreadsheet](https://s.apache.org/beam-release-validation).
  1. Vote in the release thread.


*****

## Fix any issues

Any issues identified during the community review and vote should be fixed in this step. Additionally, any JIRA issues created from the initial branch verification should be fixed.

Code changes should be proposed as standard pull requests to the `master` branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.


*****
 
## Checklist to proceed to the finalization step

1. Community votes to release the proposed candidate. Make sure:
   * there are at least three approving PMC votes;
   * enough time is given to community to vote (the usual Apache guideline is at least 72 hours not including weekends);
1. Issues identified during vote have been resolved, with fixes committed to the release branch.
1. All issues tagged with `Fix-Version` for the current release should be closed.

*****
<a class="button button--primary" href="{{'/contribute/release-guide/finalize/'|prepend:site.baseurl}}">Next Step: Finalize the Release</a>
