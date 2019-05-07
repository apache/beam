**Please** add a meaningful description for your change here

------------------------

Thank you for your contribution! Follow this checklist to help us incorporate your contribution quickly and easily:

 - [ ] [**Choose reviewer(s)**](https://beam.apache.org/contribute/#make-your-change) and mention them in a comment (`R: @username`).
 - [ ] Format the pull request title like `[BEAM-XXX] Fixes bug in ApproximateQuantiles`, where you replace `BEAM-XXX` with the appropriate JIRA issue, if applicable. This will automatically link the pull request to the issue.
 - [ ] If this contribution is large, please file an Apache [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.pdf).

Post-Commit Tests Status (on master branch)
------------------------------------------------------------------------------------------------

Lang | SDK | Apex | Dataflow | Flink | Gearpump | Samza | Spark
--- | --- | --- | --- | --- | --- | --- | ---
Go | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/) | --- | --- | --- | --- | --- | ---
Java | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Apex/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Apex/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Gearpump/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Gearpump/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/)
Python | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Python_Verify/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Python_Verify/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Python3_Verify/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Python3_Verify/lastCompletedBuild/) | --- | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/) <br> [![Build Status](https://builds.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Python_PVR_Flink_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Python_PVR_Flink_Cron/lastCompletedBuild/) | --- | --- | ---

Pre-Commit Tests Status (on master branch)
------------------------------------------------------------------------------------------------

--- |Java | Python | Go | Website
--- | --- | --- | --- | ---
Non-portable | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Java_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Java_Cron/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Python_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Python_Cron/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Go_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Go_Cron/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Website_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Website_Cron/lastCompletedBuild/) 
Portable | --- | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Portable_Python_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Portable_Python_Cron/lastCompletedBuild/) | --- | ---

See [.test-infra/jenkins/README](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md) for trigger phrase, status and link of all Jenkins jobs.
