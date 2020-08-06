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

import random
import string
import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp.dicomio import DicomSearch
  from apache_beam.io.gcp.dicomio import UploadToDicomStore
  from google.auth import default
  from google.auth.transport import requests
except ImportError:
  default = None
# pylint: enable=wrong-import-order, wrong-import-position

REGION = 'us-central1'
DATA_SET_ID = 'apache-beam-integration-testing'
HEALTHCARE_BASE_URL = 'https://healthcare.googleapis.com/v1'
GCS_BASE_URL = 'https://storage.googleapis.com/storage/v1'
PERSISTENT_DICOM_STORE_NAME = "dicom_it_persistent_store"
TEMP_BUCKET_NAME = 'temp-storage-for-dicom-io-tests'
TEMP_FILES_PATH = 'gs://' + TEMP_BUCKET_NAME
META_DATA_ALL_NAME = 'Dicom_io_it_test_data.json'
META_DATA_REFINED_NAME = 'Dicom_io_it_test_refined_data.json'
NUM_INSTANCE = 18
RAND_LEN = 15


def random_string_generator(length):
  letters_and_digits = string.ascii_letters + string.digits
  result = ''.join((random.choice(letters_and_digits) for i in range(length)))
  return result


def create_dicom_store(project_id, dataset_id, region, dicom_store_id):
  # Create a an empty DICOM store
  credential, _ = default()
  session = requests.AuthorizedSession(credential)
  api_endpoint = "{}/projects/{}/locations/{}".format(
      HEALTHCARE_BASE_URL, project_id, region)

  # base of dicomweb path.
  dicomweb_path = "{}/datasets/{}/dicomStores".format(api_endpoint, dataset_id)

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


def get_gcs_file_http(file_name):
  # Get gcs file from REST Api
  api_endpoint = "{}/b/{}/o/{}?alt=media".format(
      GCS_BASE_URL, TEMP_BUCKET_NAME, file_name)

  credential, _ = default()
  session = requests.AuthorizedSession(credential)

  response = session.get(api_endpoint)
  response.raise_for_status()
  return response.json()


@unittest.skipIf(default is None, 'GCP dependencies are not installed')
class DICOMIoIntegrationTest(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.project = self.test_pipeline.get_option('project')
    self.expected_output_all_metadata = get_gcs_file_http(META_DATA_ALL_NAME)
    self.expected_output_refined_metadata = get_gcs_file_http(
        META_DATA_REFINED_NAME)

    # create a temp Dicom store based on the time stamp
    self.temp_dicom_store = "DICOM_store_" + random_string_generator(RAND_LEN)
    create_dicom_store(self.project, DATA_SET_ID, REGION, self.temp_dicom_store)

  def tearDown(self):
    # clean up the temp Dicom store
    delete_dicom_store(self.project, DATA_SET_ID, REGION, self.temp_dicom_store)

  @attr('IT')
  def test_dicom_search_all_instances(self):
    # Search and compare the metadata of a persistent DICOM store.
    input_dict = {}
    input_dict['project_id'] = self.project
    input_dict['region'] = REGION
    input_dict['dataset_id'] = DATA_SET_ID
    input_dict['dicom_store_id'] = PERSISTENT_DICOM_STORE_NAME
    input_dict['search_type'] = "instances"

    expected_dict = {}
    expected_dict['result'] = self.expected_output_all_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    with self.test_pipeline as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_dict]), label='all search assert')

  @attr('IT')
  def test_dicom_search_refined_instances(self):
    # Refine search and compare the metadata of a persistent DICOM store.
    input_dict = {}
    input_dict['project_id'] = self.project
    input_dict['region'] = REGION
    input_dict['dataset_id'] = DATA_SET_ID
    input_dict['dicom_store_id'] = PERSISTENT_DICOM_STORE_NAME
    input_dict['search_type'] = "instances"
    input_dict['params'] = {
        'StudyInstanceUID': 'study_000000001', 'limit': 500, 'offset': 0
    }

    expected_dict = {}
    expected_dict['result'] = self.expected_output_refined_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    with self.test_pipeline as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(
          results, equal_to([expected_dict]), label='refine search assert')

  @attr('IT')
  def test_dicom_store_instance_from_gcs(self):
    # Store DICOM files to a empty DICOM store from a GCS bucket,
    # then check if the store metadata match.
    input_dict_store = {}
    input_dict_store['project_id'] = self.project
    input_dict_store['region'] = REGION
    input_dict_store['dataset_id'] = DATA_SET_ID
    input_dict_store['dicom_store_id'] = self.temp_dicom_store

    expected_output = [True] * NUM_INSTANCE

    with self.test_pipeline as p:
      gcs_path = TEMP_FILES_PATH + "/dicom_files/*"
      results = (
          p
          | fileio.MatchFiles(gcs_path)
          | fileio.ReadMatches()
          | UploadToDicomStore(input_dict_store, 'fileio')
          | beam.Map(lambda x: x['success']))
      assert_that(
          results, equal_to(expected_output), label='store first assert')

    input_dict_search = {}
    input_dict_search['project_id'] = self.project
    input_dict_search['region'] = REGION
    input_dict_search['dataset_id'] = DATA_SET_ID
    input_dict_search['dicom_store_id'] = self.temp_dicom_store
    input_dict_search['search_type'] = "instances"

    expected_dict = {}
    expected_dict['result'] = self.expected_output_all_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict_search
    expected_dict['success'] = True

    with self.test_pipeline as p:
      results = (p | beam.Create([input_dict_search]) | DicomSearch())
      assert_that(
          results, equal_to([expected_dict]), label='store second assert')


if __name__ == '__main__':
  unittest.main()
