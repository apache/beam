# <!--
#     Licensed to the Apache Software Foundation (ASF) under one
#     or more contributor license agreements.  See the NOTICE file
#     distributed with this work for additional information
#     regarding copyright ownership.  The ASF licenses this file
#     to you under the Apache License, Version 2.0 (the
#     "License"); you may not use this file except in compliance
#     with the License.  You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
# -->

from google.cloud import secretmanager
import requests
from requests.structures import CaseInsensitiveDict

def removeRunners(event,context):

  #Replace with your secret values
  project_id = "PROJECT_ID"
  token_secret = "TOKEN_SECRET"
  org_secret = "ORG_SECRET"
  runner_group_secret= "GROUP_ID"


  version_id= "latest"

  client = secretmanager.SecretManagerServiceClient()

  nameToken = f"projects/{project_id}/secrets/{token_secret}/versions/{version_id}"
  nameGroup= f"projects/{project_id}/secrets/{runner_group_secret}/versions/{version_id}"
  nameOrg= f"projects/{project_id}/secrets/{org_secret}/versions/{version_id}"
  # Access the secret version.
  responseToken = client.access_secret_version(request={"name": nameToken})
  responseGroup= client.access_secret_version(request={"name": nameGroup})
  responseOrg= client.access_secret_version(request={"name": nameOrg})

  token = responseToken.payload.data.decode("UTF-8")
  runnerGroup = responseGroup.payload.data.decode("UTF-8")
  org= responseOrg.payload.data.decode("UTF-8")

  runnersUrl=f'''https://api.github.com/orgs/{org}/actions/runner-groups/{runnerGroup}/runners'''

  headers = CaseInsensitiveDict()
  headers["Accept"] = "application/json"
  headers["Authorization"] = f'''Bearer {token}'''

  resp = requests.get(runnersUrl, headers=headers)
  resp = resp.json()
  print(resp)
  if(len(resp["runners"])):
    for runner in resp["runners"]:
      if runner["status"]=='offline':
        runner_id=runner["id"]
        delete_url=f'''https://api.github.com/orgs/{org}/actions/runners/{runner_id}'''
        print(delete_url)
        deleteResp=requests.delete(delete_url,headers=headers)
        print(deleteResp.status_code)
  return "Runners removed"