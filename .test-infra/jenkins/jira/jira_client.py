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


from jira import JIRA

class JiraClient:
  def __init__(self, options, basic_auth, project):
    self.jira = JIRA(options, basic_auth=basic_auth)
    self.project = self.jira.project(project)


  def get_issues_all(self):
    # TODO
    pass


  def get_issue(self, key):
    try:
      issue = self.jira.issue(key)
    except Exception, e:
      raise
    return issue


  def create_issue(self, summary, components, description='', issuetype='Bug', assignee=None, parent_key=None):
    issue_dict = {
      'project': {'key': self.project},
      'summary': summary,
      'description': description,
      'issuetype': {'name': issuetype},
      'components': [{'name': components}],
    }
    if assignee is not None:
      issue_dict['assignee'] = {'name': assignee}
    if parent_key is not None:
      issue_dict['parent'] = {'key': parent_key}

    new_issue = jira.create_issue(fields = issue_dict)


  def comment_issue(self):
    #TODO:
    pass

  def close_issue(self):
    #TODO:
    pass
