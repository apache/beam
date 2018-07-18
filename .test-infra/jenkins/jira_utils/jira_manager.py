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
from datetime import datetime
from jira_client import JiraClient

_JIRA_PROJECT_NAME = 'BEAM'
_JIRA_COMPONENT = 'dependencies'

class JiraManager:

  def __init__(self, jira_url, jira_username, jira_password, owners_file, sdk_type='Java'):
    options = {
      'server': jira_url
    }
    basic_auth = (jira_username, jira_password)
    self.jira = JiraClient(options, basic_auth, _JIRA_PROJECT_NAME)
    with open(owners_file) as f:
      owners = yaml.load(f)
    self.owners_map = owners['deps']
    logging.getLogger().setLevel(logging.INFO)


  def _create_issue(self, dep_name, dep_latest_version, is_subtask=False, parent_key=None):
    """
    Create a new issue or subtask
    Args:
      dep_name
      dep_latest_version
      is_subtask
      parent_key - only required if the 'is_subtask'is true.
    """
    logging.info("Creating a new JIRA issue to track {0} upgrade process").format(dep_name)
    assignee, owners = self._find_assignees(dep_name)
    summary = 'Beam Dependency Update Request: ' + dep_name
    description = """\n\n {0} \n
        Please review and upgrade the {1} to the latest version {2} \n 
        cc: """.format(
        datetime.today(),
        dep_name,
        dep_latest_version
    )
    for owner in owners:
      description.append("[~{0}],".format(owner))
    try:
      if not is_subtask:
        self.jira.create_issue(summary, _JIRA_COMPONENT, description=description, assignee=assignee)
      else:
        self.jira.create_issue(summary, _JIRA_COMPONENT, description=description, assignee=assignee, parent_key=parent_key)
    except Exception as e:
      logging.error("Error while creating issue: "+ str(e))


  def _search_issues(self, dep_name):

    pass


  def _append_descriptions(self, issue, dep_name, dep_latest_version):
    logging.info("Updating JIRA issue to {0} track {1} upgrade process").format(
        issue.key['name'],
        dep_name)
    description = issue.description + """\n\n {0} \n
        Please review and upgrade the {1} to the latest version {2} \n 
        cc: """.format(
        datetime.today(),
        dep_name,
        dep_latest_version
    )
    _, owners = self._find_assignees(dep_name)
    for owner in owners:
      description.append("[~{0}],".format(owner))
    try:
      self.jira.update_issue(issue, description=description)
    except Exception as e:
      logging.error("Error while updating issue: "+ str(e))



  def _find_owners(self, dep_name):
    try:
      dep_info = self.owners_map[dep_name]
      owners = dep_info['owners']
      if not owners:
        logging.info("Could not find owners for " + dep_name)
        return None, []
    except KeyError:
      logging.info("Could not find {0} in the ownership configurations.".format(dep_name))
      return None, []
    except Exception as e:
      logging.error("Error while finding dependency owners: "+ str(e))
      return None, None

    logging.info("Found owners of {0}: {1}".format(dep_name, owners))
    owners = owners.split(',')
    primary = owners[0]
    del owners[0]
    return primary, owners


  def run(self, dep_name, dep_latest_version, group_id=None, artifact_id=None):

    pass
