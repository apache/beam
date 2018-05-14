#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# This script checks that the release instructions in https://beam.apache.org/contribute/release-guide
# have been followed.

echo 'Check preparation of a new release for Apache Beam.'
echo ' '

# Load the functions in release_helper.sh.
. $(dirname "${BASH_SOURCE[0]}")/release_helper.sh 

# Load previous answers, to avoid re-prompting.
ANSWER_FILE=~/.prepare_beam_release_answers.txt
load_previous_answers

# Check the needed software is installed.
check_software gpg
check_software git
check_software mvn
check_software svn
check_software gpg-agent
check_software python

check_gpg_key
check_access_to_nexus

ensure_yes release_proposed "Has a release been proposed to the @dev list" "
  Deciding to release and selecting a Release Manager is the first step of
  the release process. This is a consensus-based decision of the entire
  community. Anybody can propose a release on the dev@ mailing list, giving
  a solid argument and nominating a committer as the Release Manager
  (including themselves). There’s no formal process, no vote requirements,
  and no timing requirements. Any objections should be resolved by consensus
  before starting the release."

ensure_set beam_version "What version number will be this release"
check_beam_version_in_jira "${beam_version}" "current"
beam_version_id=$found_beam_version_id

ensure_yes website_setup "Have you set up access to the beam website?" "
  You need to prepare access to the beam website to push changes there"

set_next_version
ensure_yes next_version_looks_ok "Will the next version (after ${beam_version}) be version ${next_version}" "
  When contributors resolve an issue in JIRA, they are tagging it with a
  release that will contain their changes. With the release currently underway,
  new issues should be resolved against a subsequent future release."

check_beam_version_in_jira "${next_version}" "next"

check_no_unresolved_issues

release_page="https://issues.apache.org/jira/projects/BEAM/versions/${beam_version_id}"
ensure_yes release_notes_reviewed "Have you reviewed and edited the release notes?" "
  JIRA automatically generates Release Notes based on the Fix Version field applied to issues.
  Release Notes are intended for Beam users (not Beam committers/contributors).
  You should ensure that Release Notes are informative and useful.
  The release notes are linked from ${release_page}"

ensure_yes release_build_works "Have you run a release build with mvn -Prelease clean install?" "
  Before creating a release branch, ensure that the release build works and javadoc in sdks/java/javadoc
  looks ok"

check_release_branch_created

check_python_version master "${next_version}.dev"
check_python_version "release-${beam_version}" "${beam_version}"

check_java_version master beam-master-YYYYMMDD
check_java_version "release-${beam_version}" "beam-${beam_version}"

cleanup

echo ""
echo "Script complete, but there are more steps at https://beam.apache.org/contribute/release-guide"
echo "To start with a clean state, rm ${ANSWER_FILE} before re-running"
