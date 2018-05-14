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
# This file contains helper functions for checking and performing Apache Beam releases.

# Fail the script and exit with an error.
function fail {
  echo ""
  echo "Failed: $1"
  echo "Correct this, then re-run this script."
  echo "To start with a clean state, rm ${ANSWER_FILE} before re-running"
  echo "For more details, see https://beam.apache.org/contribute/release-guide"
  cleanup
  exit 1
}

function success {
  echo "Success: ${1}"
}

function warning {
  echo "Warning: ${1}"
}

# Check that needed software is installed.
function check_software {
  command -v "${1}" > /dev/null || fail "${1} not installed"
  success "${1} is installed"
}

# Load previous answers; these are stored as shell script set statements.
function load_previous_answers {
  if [[ -f $ANSWER_FILE ]]; then
    echo "Loading previous answers"
    . $ANSWER_FILE
  else
    echo "# prepare beam release answers" > $ANSWER_FILE
  fi
}

# Check that an answer is yes/y.
function check_yes {
  varname=$1
  question=$2
  if [[ "${!varname}" == "y" ]] || [[ "${!varname}" == "yes" ]]; then
    success "${question}: ${!varname}"
    return 0
  else
    return 1
  fi
}

# Check that a variable is already set.
function check_set {
  varname=$1
  question=$2
  if [[ "${!varname}" != "" ]]; then
    success "${varname}: ${!varname}"
    return 0
  else
    return 1
  fi
}

# Prompt the user and ensure that the answer is yes,
# if so store it to the answer file to prevent re-prompting.
function ensure_yes {
  varname=$1
  question=$2
  description=$3
  check_yes "${varname}" "${question}" && return
  echo ""
  echo "${description}"
  echo ""
  echo -n "${question} [y/n]: "
  read "${varname}"
  echo "${varname}='${!varname}'" >> $ANSWER_FILE
  check_yes "${varname}" "${question}" || fail "Need yes: ${question}"
}

# Prompt the user and ensure that a value is set,
# if so store it to the answer file to prevent re-prompting.
function ensure_set {
  varname=$1
  question=$2
  description=$3
  check_set "${varname}" "${question}" && return
  echo ""
  echo -n "${question}: "
  read  "${varname}"
  echo "${varname}='${!varname}'" >> $ANSWER_FILE
  check_set "${varname}" "${question}" || fail "Need: ${question}"
}

# Check that the gpg key is valid.
function check_gpg_key {
  key=$(git config --get user.signingkey)
  if [[ "${key}" == "" ]]; then
    fail "git config user.signing key not set"
  fi
  details=$(gpg --list-keys "${key}")
  if  [[ "${details}" =~ "apache.org" ]]; then
    success "found gpg key for apache.org"
  else
    fail "GPG key ${key} is not for apache.org"
  fi
  ps cax | grep gpg-agent > /dev/null || fail "gpg-agent should be running"
  success 'gpg-agent is running'
}

# Check that nexus has been configured for maven.
function check_access_to_nexus {
  grep apache.releases.http ~/.m2/settings.xml > /dev/null || \
    fail "Can't find apache.releases.https in ~/.m2/settings.xml"

  grep apache.snapshots.http ~/.m2/settings.xml > /dev/null || \
    fail "Can't find apache.snapshots.https in ~/.m2/settings.xml"

  success 'Nexus settings found in ~/.m2/settings.xml'
}

# Set the next version, like 2.6.0 is after 2.5.0.
function set_next_version {
  next_version=$(echo "${beam_version}" | awk '{a=$1; split(a,b,"."); print b[1] "." 1+b[2] "." b[3]}')
}

# Parse data from a json response; this is fragile but should be ok for this script.
function parse_json {
  json=$1
  callback=$2
  readarray -t lines <<< "${json}"
  for line in "${lines[@]}"
  do
    if [[ "${line}" =~ ":" ]]; then
      key=$(echo "${line}" | sed 's/^ *//' | sed 's/:.*$//' | sed 's/"//g')
      value=$(echo "${line}" | sed 's/^.*: *"*//' | sed 's/"*,* *$//')
      "${callback}" "${key}" "${value}"
    fi
  done
}

# Get the beam version id matching a version from the jira response.
function json_callback_version {
  key=$1
  value=$2
  if [[ "${key}" == "id" ]]; then
    id=$value
  fi
  if [[ "${key}" == "name" ]] && [[ "${value}" == "${version_to_find}" ]]; then
    found_beam_version_id=$id
  fi
}

# Check that a beam version is set in jira.
function check_beam_version_in_jira {
  version_to_find="${1}"
  current_or_next="${2}"
  data=$(curl https://issues.apache.org/jira/rest/api/2/project/BEAM/versions 2> /dev/null | \
    python -m  json.tool)
  found_beam_version_id=""
  parse_json "${data}" json_callback_version
  if [[ "${found_beam_version_id}" == "" ]]; then
    fail "Beam ${current_or_next} version ${version_to_find} not in issues.apache.org/jira, but should be added."

  else
    success "Found ${current_or_next} version ${version_to_find} in apache issues"
  fi
}

# For unresolved jira issues, ask if it is ok to ignore them for now.
function json_callback_issue_ok {
  key=$1
  value=$2
  if [[ "${key}" == "summary" ]]; then
    summary=$value
  fi
  if [[ "${key}" == "key" ]]; then
    id=$value
    url="https://issues.apache.org/jira/browse/${id}"
    varname=$(echo "ignore_${id}" | sed 's/-/_/g')
    ensure_yes "${varname}" "Can issue ${id} be ignored for now?" "
      All issues in ${beam_version} should be resolved or moved to ${next_version}.
      This issue is unresolved:
      ${id} ${summary}
      ${url}
      "
  fi
}

# Capture the number of unresolved issues.
function json_callback_issue_total {
  key=$1
  value=$2
  if [[ "${key}" == "total" ]]; then
    total_issues=$value
  fi
}

# Check that there are no unresolved issues, or ask if they can be ignored. 
function check_no_unresolved_issues {
  jql="project=BEAM%20AND%20resolution=Unresolved%20AND%20fixVersion=${beam_version}"
  results=$(curl "https://issues.apache.org/jira/rest/api/2/search?jql=${jql}&fields=summary" 2> /dev/null | \
     python -m json.tool)
  parse_json "${results}" json_callback_issue_total
  if [[ "${total_issues}" == "0" ]]; then
    success "There are no unresolved issues for ${beam_version}!"
    return
  fi
  warning "There are ${total_issues} unresolved issues for ${beam_version}!"
  parse_json "${results}" json_callback_issue_ok
}

# Check that the release branch exists in git.
function check_release_branch_created {
  git ls-remote https://github.com/apache/beam.git | \
    grep "refs/heads/release-${beam_version}" > /dev/null || \
    fail "Version release-${beam_version} should be created"
  success "Found git version release-${beam_version}"
}

# Cleanup and delete the git repository copy, if available.
function cleanup {
  if [[ "${beam_git}" != "" ]]; then
    rm -rf "${beam_git}"
  fi
}

# Clone the git repo into a temp directory.
function clone_beam_git {
  if [[ "${beam_git}" != "" ]]; then
    return
  fi
  beam_git=$(mktemp -t -d beam.XXXXXX)
  git clone https://github.com/apache/beam "${beam_git}" 2> /dev/null
}

# Check that the python version is as expected.
function check_python_version {
  branch=$1
  expected_version=$2
  clone_beam_git
  pushd "${beam_git}" > /dev/null  && git checkout "${branch}" > /dev/null 2>&1  && popd > /dev/null
  ver=$(grep __version__  "${beam_git}/sdks/python/apache_beam/version.py" | sed 's/^.*= //')
  if [[ "${ver}" != "'${expected_version}'" ]]; then
    fail "Expected python version ${expected_version} on branch ${branch} but found ${ver}."
  fi
  success "Found python version ${ver} on branch ${branch}."
}

# Check that the java version is as expected.
function check_java_version {
  branch=$1
  expected_version=$2
  clone_beam_git
  pushd "${beam_git}" > /dev/null  && git checkout "${branch}" > /dev/null 2>&1  && popd > /dev/null
  ver=$(grep dataflow.container_version "${beam_git}/runners/google-cloud-dataflow-java/pom.xml" | \
    sed 's/^.*>\(.*\)<.*$/\1/' | sed 's/\-[0-9]\{8\}/-YYYYMMDD/')
  if [[ "${ver}" != "${expected_version}" ]]; then
    fail "Expected java version ${expected_version} on branch ${branch} but found ${ver}."
  fi
  success "Found java version ${ver} on branch ${branch}."
}
