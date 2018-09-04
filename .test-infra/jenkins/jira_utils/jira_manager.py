from __future__ import print_function
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import yaml
import traceback
from datetime import datetime
from jira_client import JiraClient

_JIRA_PROJECT_NAME = 'BEAM'
_JIRA_COMPONENT = 'dependencies'
_ISSUE_SUMMARY_PREFIX = 'Beam Dependency Update Request: '

class JiraManager:

  def __init__(self, jira_url, jira_username, jira_password, owners_file):
    options = {
      'server': jira_url
    }
    basic_auth = (jira_username, jira_password)
    self.jira = JiraClient(options, basic_auth, _JIRA_PROJECT_NAME)
    with open(owners_file) as f:
      owners = yaml.load(f)
    self.owners_map = owners['deps']
    logging.getLogger().setLevel(logging.INFO)


  def run(self, dep_name, dep_latest_version, sdk_type, group_id=None):
    """
    Manage the jira issue for a dependency
    Args:
      dep_name,
      dep_latest_version,
      sdk_type: Java, Python
      group_id (optional): only required for Java dependencies
    Return: Jira Issue
    """
    logging.info("Start handling the JIRA issues for {0} dependency: {1} {2}".format(
        sdk_type, dep_name, dep_latest_version))
    try:
      # find the parent issue for Java deps base on the groupID
      parent_issue = None
      if sdk_type == 'Java':
        summary = _ISSUE_SUMMARY_PREFIX + group_id
        parent_issues = self._search_issues(summary)
        for i in parent_issues:
          if i.fields.summary == summary:
            parent_issue = i
            break
        # Create a new parent issue if no existing found
        if not parent_issue:
          logging.info("""Did not find existing issue with name {0}. \n 
            Created a parent issue for {1}""".format(summary, group_id))
          try:
            parent_issue = self._create_issue(group_id, None)
            print(parent_issue.key)
          except:
            logging.error("""Failed creating a parent issue for {0}.
              Stop handling the JIRA issue for {1}, {2}""".format(group_id, dep_name, dep_latest_version))
            return
        # Reopen the existing parent issue if it was closed
        elif (parent_issue.fields.status.name != 'Open' and
          parent_issue.fields.status.name != 'Reopened'):
          logging.info("""The parent issue {0} is not opening. Attempt reopening the issue""".format(parent_issue.key))
          try:
            self.jira.reopen_issue(parent_issue)
          except:
            traceback.print_exc()
            logging.error("""Failed reopening the parent issue {0}.
              Stop handling the JIRA issue for {1}, {2}""".format(parent_issue.key, dep_name, dep_latest_version))
            return
        logging.info("Found the parent issue {0}. Continuous to create or update the sub-task for {1}".format(parent_issue.key, dep_name))
      # creating a new issue/sub-task or updating on the existing issue of the dep
      summary =  _ISSUE_SUMMARY_PREFIX + dep_name + " " + dep_latest_version
      issues = self._search_issues(summary)
      issue = None
      for i in issues:
        if i.fields.summary == summary:
          issue = i
          break
      if not issue:
        if sdk_type == 'Java':
          issue = self._create_issue(dep_name, dep_latest_version, is_subtask=True, parent_key=parent_issue.key)
        else:
          issue = self._create_issue(dep_name, dep_latest_version)
        logging.info('Created a new issue {0} of {1} {2}'.format(issue.key, dep_name, dep_latest_version))
      elif issue.fields.status.name == 'Open' or issue.fields.status.name == 'Reopened':
        self._append_descriptions(issue, dep_name, dep_latest_version)
        logging.info('Updated the existing issue {0} of {1} {2}'.format(issue.key, dep_name, dep_latest_version))
      return issue
    except:
      raise


  def _create_issue(self, dep_name, dep_latest_version, is_subtask=False, parent_key=None):
    """
    Create a new issue or subtask
    Args:
      dep_name,
      dep_latest_version,
      is_subtask,
      parent_key: only required if the 'is_subtask'is true.
    """
    logging.info("Creating a new JIRA issue to track {0} upgrade process".format(dep_name))
    assignee, owners = self._find_owners(dep_name)
    summary =  _ISSUE_SUMMARY_PREFIX + dep_name
    if dep_latest_version:
      summary = summary + " " + dep_latest_version
    description = """\n\n{0}\n
        Please review and upgrade the {1} to the latest version {2} \n 
        cc: """.format(
        datetime.today(),
        dep_name,
        dep_latest_version
    )
    for owner in owners:
      description += "[~{0}], ".format(owner)
    try:
      if not is_subtask:
        issue = self.jira.create_issue(summary, [_JIRA_COMPONENT], description, assignee=assignee)
      else:
        issue = self.jira.create_issue(summary, [_JIRA_COMPONENT], description, assignee=assignee, parent_key=parent_key)
    except Exception as e:
      logging.error("Failed creating issue: "+ str(e))
      raise e
    return issue


  def _search_issues(self, summary):
    """
    Search issues by using issues' summary.
    Args:
      summary: a string
    Return:
      A list of issues
    """
    try:
      issues = self.jira.get_issues_by_summary(summary)
    except Exception as e:
      logging.error("Failed searching issues: "+ str(e))
      return []
    return issues


  def _append_descriptions(self, issue, dep_name, dep_latest_version):
    """
    Add descriptions on an existing issue.
    Args:
      issue: Jira issue
      dep_name
      dep_latest_version
    """
    logging.info("Updating JIRA issue {0} to track {1} upgrade process".format(
        issue.key,
        dep_name))
    description = issue.fields.description + """\n\n{0}\n
        Please review and upgrade the {1} to the latest version {2} \n 
        cc: """.format(
        datetime.today(),
        dep_name,
        dep_latest_version
    )
    _, owners = self._find_owners(dep_name)
    for owner in owners:
      description += "[~{0}], ".format(owner)
    try:
      self.jira.update_issue(issue, description=description)
    except Exception as e:
      traceback.print_exc()
      logging.error("Failed updating issue: "+ str(e))


  def _find_owners(self, dep_name):
    """
    Find owners for a dependency/
    Args:
      dep_name
    Return:
      primary: The primary owner of the dep. The Jira issue will be assigned to the primary owner.
      others: A list of other owners of the dep. Owners will be cc'ed in the description.
    """
    try:
      dep_info = self.owners_map[dep_name]
      owners = dep_info['owners']
      if not owners:
        logging.warning("Could not find owners for " + dep_name)
        return None, []
    except KeyError:
      traceback.print_exc()
      logging.warning("Could not find the dependency info of {0} in the OWNERS configurations.".format(dep_name))
      return None, []
    except Exception as e:
      traceback.print_exc()
      logging.error("Failed finding dependency owners: "+ str(e))
      return None, None

    logging.info("Found owners of {0}: {1}".format(dep_name, owners))
    owners = owners.split(',')
    owners = map(str.strip, owners)
    owners = list(filter(None, owners))
    primary = owners[0]
    del owners[0]
    return primary, owners
