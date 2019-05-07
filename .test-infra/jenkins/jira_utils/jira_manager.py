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
import os
import traceback
import yaml
import dependency_check.version_comparer as version_comparer

from datetime import datetime
from jira_client import JiraClient


_JIRA_PROJECT_NAME = 'BEAM'
_JIRA_COMPONENT = 'dependencies'
_ISSUE_SUMMARY_PREFIX = 'Beam Dependency Update Request: '
_ISSUE_REOPEN_DAYS = 180

class JiraManager:

  def __init__(self, jira_url, jira_username, jira_password, owners_file):
    options = {
      'server': jira_url
    }
    basic_auth = (jira_username, jira_password)
    self.jira = JiraClient(options, basic_auth, _JIRA_PROJECT_NAME)
    with open(owners_file) as f:
      owners = yaml.load(f, Loader=yaml.BaseLoader)
    self.owners_map = owners['deps']
    logging.getLogger().setLevel(logging.INFO)


  def run(self, dep_name,
      dep_current_version,
      dep_latest_version,
      sdk_type,
      group_id=None):
    """
    Manage the jira issue for a dependency
    Args:
      dep_name,
      dep_current_version,
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
            parent_issue = self._create_issue(group_id, None, None)
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
      summary =  _ISSUE_SUMMARY_PREFIX + dep_name
      issues = self._search_issues(summary)
      issue = None
      for i in issues:
        if i.fields.summary == summary:
          issue = i
          break
      # Create a new JIRA if no existing one.
      if not issue:
        if sdk_type == 'Java':
          issue = self._create_issue(dep_name, dep_current_version, dep_latest_version, is_subtask=True, parent_key=parent_issue.key)
        else:
          issue = self._create_issue(dep_name, dep_current_version, dep_latest_version)
        logging.info('Created a new issue {0} of {1} {2}'.format(issue.key, dep_name, dep_latest_version))
      # Add descriptions in to the opening issue.
      elif issue.fields.status.name == 'Open' or issue.fields.status.name == 'Reopened':
        self._append_descriptions(issue, dep_name, dep_current_version, dep_latest_version)
        logging.info('Updated the existing issue {0} of {1} {2}'.format(issue.key, dep_name, dep_latest_version))
      # Check if we need reopen the issue if it was closed. If so, reopen it then add descriptions.
      elif self._need_reopen(issue, dep_latest_version):
        self.jira.reopen_issue(issue)
        self._append_descriptions(issue, dep_name, dep_current_version, dep_latest_version)
        logging.info("Reopened the issue {0} for {1} {2}".format(issue.key, dep_name, dep_latest_version))
      return issue
    except:
      raise


  def _create_issue(self, dep_name, dep_current_version, dep_latest_version, is_subtask=False, parent_key=None):
    """
    Create a new issue or subtask
    Args:
      dep_name,
      dep_latest_version,
      is_subtask,
      parent_key: only required if the 'is_subtask'is true.
    """
    logging.info("Creating a new JIRA issue to track {0} upgrade process".format(dep_name))
    summary =  _ISSUE_SUMMARY_PREFIX + dep_name
    description = self._create_descriptions(dep_name, dep_current_version, dep_latest_version)
    try:
      if not is_subtask:
        issue = self.jira.create_issue(summary, [_JIRA_COMPONENT], description)
      else:
        issue = self.jira.create_issue(summary, [_JIRA_COMPONENT], description, parent_key=parent_key)
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


  def _append_descriptions(self, issue, dep_name, dep_current_version, dep_latest_version):
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
    description = self._create_descriptions(dep_name, dep_current_version, dep_latest_version, issue=issue)
    try:
      self.jira.update_issue(issue, description=description)
    except Exception as e:
      traceback.print_exc()
      logging.error("Failed updating issue: "+ str(e))


  def _create_descriptions(self, dep_name, dep_current_version, dep_latest_version, issue = None):
    """
    Create descriptions for JIRA issues.
    Args:
      dep_name
      dep_latest_version
      issue
    """
    description = ""
    if issue:
      description = issue.fields.description
    description +=  """\n\n ------------------------- {0} -------------------------\n
        Please consider upgrading the dependency {1}. \n
        The current version is {2}. The latest version is {3} \n
        cc: """.format(
        datetime.today(),
        dep_name,
        dep_current_version,
        dep_latest_version
    )
    owners = self._find_owners(dep_name)
    for owner in owners:
      description += "[~{0}], ".format(owner)
    description += ("\n Please refer to "
    "[Beam Dependency Guide |https://beam.apache.org/contribute/dependencies/]" 
    "for more information. \n"
    "Do Not Modify The Description Above. \n")
    return description


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
        return []
    except KeyError:
      traceback.print_exc()
      logging.warning("Could not find the dependency info of {0} in the OWNERS configurations.".format(dep_name))
      return []
    except Exception as e:
      traceback.print_exc()
      logging.error("Failed finding dependency owners: "+ str(e))
      return None

    logging.info("Found owners of {0}: {1}".format(dep_name, owners))
    owners = owners.split(',')
    owners = map(str, owners)
    owners = map(str.strip, owners)
    owners = list(filter(None, owners))
    return owners


  def _need_reopen(self, issue, dep_latest_version):
    """
    Return a boolean that indicates whether reopen the closed issue.
    """
    # Check if the issue was closed with a "fix version/s"
    # Reopen the issue if it hits the next release version.
    next_release_version = self._get_next_release_version()
    for fix_version in issue.fields.fixVersions:
      if fix_version.name in next_release_version:
        return True

    # Check if there is other new versions released.
    # Reopen the issue if 3 new versions have been released in 6 month since closure.
    try:
      if issue.fields.resolutiondate:
        closing_date = datetime.strptime(issue.fields.resolutiondate[:19], "%Y-%m-%dT%H:%M:%S")
        if (datetime.today() - closing_date).days >= _ISSUE_REOPEN_DAYS:
          # Extract the previous version when JIRA closed.
          descriptions = issue.fields.description.splitlines()
          descriptions = descriptions[len(descriptions)-5]
          # The version info has been stored in the JIRA description in a specific format.
          # Such as "Please review and upgrade the <dep name> to the latest version <version>"
          previous_version = descriptions.split("The latest version is", 1)[1].strip()
          if version_comparer.compare_dependency_versions(previous_version, dep_latest_version):
            return True
    except Exception as e:
      traceback.print_exc()
      logging.error("Failed deciding to reopen the issue." + str(e))
      return False

    return False


  def _get_next_release_version(self):
    """
    Return the incoming release version from sdks/python/apache_beam/version.py
    """
    global_names = {}
    exec(
      open(os.path.join(
          os.path.dirname(os.path.abspath(__file__)),
          '../../../sdks/python/',
          'apache_beam/version.py')
      ).read(),
      global_names
    )
    return global_names['__version__']
