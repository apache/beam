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

# pytype: skip-file

from google.auth import default
from google.auth.transport import requests


class DicomApiHttpClient:
  """DICOM api client that talk to api via http request"""
  healthcare_base_url = "https://healthcare.googleapis.com/v1"
  session = None

  def get_session(self, credential):
    if self.session:
      return self.session

    # if the credential is not provided, use the default credential.
    if not credential:
      credential, _ = default()
    new_seesion = requests.AuthorizedSession(credential)
    self.session = new_seesion
    return new_seesion

  def qido_search(
      self,
      project_id,
      region,
      dataset_id,
      dicom_store_id,
      search_type,
      params=None,
      credential=None):
    """function for searching a DICOM store"""

    # sending request to the REST healthcare api.
    api_endpoint = "{}/projects/{}/locations/{}".format(
        self.healthcare_base_url, project_id, region)

    # base of dicomweb path.
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/{}".format(
        api_endpoint, dataset_id, dicom_store_id, search_type)

    # Make an authenticated API request
    session = self.get_session(credential)
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}
    page_size = 500

    if params and 'limit' in params:
      page_size = params['limit']
    elif params:
      params['limit'] = page_size
    else:
      params = {'limit': page_size}

    offset = 0
    output = []
    # iterate to get all the results
    while True:
      params['offset'] = offset
      response = session.get(dicomweb_path, headers=headers, params=params)
      status = response.status_code
      if status != 200:
        if offset == 0:
          return [], status
        params['offset'] = offset - 1
        params['limit'] = 1
        response = session.get(dicomweb_path, headers=headers, params=params)
        check_status = response.status_code
        if check_status == 200:
          # if the number of results equals to page size
          return output, check_status
        else:
          # something wrong with the request or server
          return [], status
      results = response.json()
      output += results
      if len(results) < page_size:
        # got all the results, return
        break
      offset += len(results)

    return output, status

  def dicomweb_store_instance(
      self,
      project_id,
      region,
      dataset_id,
      dicom_store_id,
      dcm_file,
      credential=None):
    """function for storing an instance."""

    api_endpoint = "{}/projects/{}/locations/{}".format(
        self.healthcare_base_url, project_id, region)

    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies".format(
        api_endpoint, dataset_id, dicom_store_id)

    # Make an authenticated API request
    session = self.get_session(credential)
    content_type = "application/dicom"
    headers = {"Content-Type": content_type}

    response = session.post(dicomweb_path, data=dcm_file, headers=headers)

    return None, response.status_code
