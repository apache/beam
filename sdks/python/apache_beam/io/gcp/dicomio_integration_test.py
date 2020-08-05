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

"""
Integration test for Google Cloud DICOM IO connector.
"""
# pytype: skip-file

from __future__ import absolute_import

import json
import time
import unittest

from google.auth import default
from google.auth.transport import requests
from google.cloud import storage
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.dicomio import DicomSearch
from apache_beam.io.gcp.dicomio import UploadToDicomStore
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

REGION = 'us-central1'
DATA_SET_ID = 'apache-beam-integration-testing'
HEALTHCARE_BASE_URL = 'https://healthcare.googleapis.com/v1'
PERSISTENT_DICOM_STORE_NAME = "dicom_it_persistent_store"
DICOM_FILES_PATH = "gs://temp-storage-for-dicom-io-tests/dicom_files"
NUM_INSTANCE = 18


def create_dicom_store(project_id, dataset_id, region, dicom_store_id):
  # Create a an empty DICOM store
  credential, _ = default()
  session = requests.AuthorizedSession(credential)
  api_endpoint = "{}/projects/{}/locations/{}".format(
      HEALTHCARE_BASE_URL, project_id, region)

  # base of dicomweb path.
  dicomweb_path = "{}/datasets/{}/dicomStores".format(
      api_endpoint, dataset_id)

  response = session.post(
      dicomweb_path, params={"dicomStoreId": dicom_store_id})
  response.raise_for_status()
  return response.status_code

def delete_dicom_store(project_id, dataset_id, region, dicom_store_id):
  # Delete an existing DICOM store
  credential, _ = default()
  session = requests.AuthorizedSession(credential)
  api_endpoint = "{}/projects/{}/locations/{}".format(
      HEALTHCARE_BASE_URL, project_id, region)

  # base of dicomweb path.
  dicomweb_path = "{}/datasets/{}/dicomStores/{}".format(
      api_endpoint, dataset_id, dicom_store_id)

  response = session.delete(dicomweb_path)
  response.raise_for_status()
  return response.status_code


class DICOMIoIntegrationTest(unittest.TestCase):
  expected_output_metadata = None

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')

    # create a temp Dicom store based on the time stamp
    self.temp_dicom_store = "DICOM_store_" + str(time.time())
    create_dicom_store(
        self.project, DATA_SET_ID, REGION, self.temp_dicom_store)
    client = storage.Client()
    bucket = client.get_bucket('temp-storage-for-dicom-io-tests')
    blob = bucket.blob('meta_data_json/Dicom_io_it_test_data.json')
    data = json.loads(blob.download_as_string())
    self.expected_output_metadata = data

  def tearDown(self):
    # clean up the temp Dicom store
    delete_dicom_store(
        self.project, DATA_SET_ID, REGION, self.temp_dicom_store)

  @attr('IT')
  def test_dicom_search(self):
    # Search and compare the metadata of a persistent DICOM store.
    input_dict = {}
    input_dict['project_id'] = self.project
    input_dict['region'] = REGION
    input_dict['dataset_id'] = DATA_SET_ID
    input_dict['dicom_store_id'] = PERSISTENT_DICOM_STORE_NAME
    input_dict['search_type'] = "instances"

    expected_dict = {}
    expected_dict['result'] = self.expected_output_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_dict]))

  @attr('IT')
  def test_dicom_store_instance(self):
    # Store DICOM files to a empty DICOM store from a GCS bucket,
    # then check if the store metadata match.
    input_dict = {}
    input_dict['project_id'] = self.project
    input_dict['region'] = REGION
    input_dict['dataset_id'] = DATA_SET_ID
    input_dict['dicom_store_id'] = self.temp_dicom_store
    input_dict['search_type'] = "instances"

    expected_dict = {}
    expected_dict['result'] = self.expected_output_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    with TestPipeline() as p:
      gcs_path = DICOM_FILES_PATH + "/*"
      results = (
          p
          | fileio.MatchFiles(gcs_path)
          | fileio.ReadMatches()
          | UploadToDicomStore(input_dict, 'fileio')
          | beam.Map(lambda x: x['success']))
      assert_that(results, equal_to([True] * NUM_INSTANCE))

    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_dict]))


if __name__ == '__main__':
  unittest.main()
