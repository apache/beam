# coding=utf-8
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

"""Unit tests for PubSub sources and sinks."""

# pytype: skip-file

from __future__ import absolute_import

import json
import os
import unittest
from unittest.mock import patch

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.dicomio import DicomSearch
from apache_beam.io.gcp.dicomio import DicomStoreInstance
from apache_beam.io.gcp.dicomio import PubsubToQido
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FakeHttpClient():
  # a fake http client that talks directly to a in-memory dicom store
  def __init__(self):
    # set 5 fake dicom instances
    dicom_metadata = []
    dicom_metadata.append({
        'PatientName': 'Albert', 'Age': 21, 'TestResult': 'Positive'
    })
    dicom_metadata.append({
        'PatientName': 'Albert', 'Age': 21, 'TestResult': 'Negative'
    })
    dicom_metadata.append({
        'PatientName': 'Brian', 'Age': 20, 'TestResult': 'Positive'
    })
    dicom_metadata.append({
        'PatientName': 'Colin', 'Age': 25, 'TestResult': 'Negative'
    })
    dicom_metadata.append({
        'PatientName': 'Daniel', 'Age': 22, 'TestResult': 'Negative'
    })
    dicom_metadata.append({
        'PatientName': 'Eric', 'Age': 50, 'TestResult': 'Negative'
    })
    self.dicom_metadata = dicom_metadata
    # ids for this dicom sotre
    self.project_id = 'test_project'
    self.region = 'test_region'
    self.dataset_id = 'test_dataset_id'
    self.dicom_store_id = 'test_dicom_store_id'

  def qido_search(
      self,
      project_id,
      region,
      dataset_id,
      dicom_store_id,
      search_type,
      params=None,
      credential=None):
    # qido search function for this fake client
    if project_id != self.project_id or region != self.region or \
     dataset_id != self.dataset_id or dicom_store_id != self.dicom_store_id:
      return [], 204
    # only supports all instance search in this client
    if not params:
      return self.dicom_metadata, 200
    # only supports patient name filter in this client
    patient_name = params['PatientName']
    out = []
    for meta in self.dicom_metadata:
      if meta['PatientName'] == patient_name:
        out.append(meta)
    return out, 200

  def dicomweb_store_instance(
      self,
      project_id,
      region,
      dataset_id,
      dicom_store_id,
      dcm_file,
      credential=None):
    if project_id != self.project_id or region != self.region or \
     dataset_id != self.dataset_id or dicom_store_id != self.dicom_store_id:
      return [], 204
    # convert the bytes file back to dict
    string_array = dcm_file.decode('utf-8')
    metadata_dict = json.loads(string_array)
    self.dicom_metadata.append(metadata_dict)
    return None, 200


class TestPubsubToQido(unittest.TestCase):
  valid_pubsub_string = (
      "projects/PROJECT_ID/locations/LOCATION/datasets"
      "/DATASET_ID/dicomStores/DICOM_STORE_ID/dicomWeb/"
      "studies/STUDY_UID/series/SERIES_UID/instances/INSTANCE_UID")
  expected_valid_pubsub_dict = {
      'result': {
          'project_id': 'PROJECT_ID',
          'region': 'LOCATION',
          'dataset_id': 'DATASET_ID',
          'dicom_store_id': 'DICOM_STORE_ID',
          'search_type': 'instances',
          'params': {
              'StudyInstanceUID': 'STUDY_UID',
              'SeriesInstanceUID': 'SERIES_UID',
              'SOPInstanceUID': 'INSTANCE_UID'
          }
      },
      'input': valid_pubsub_string,
      'success': True
  }
  invalid_pubsub_string = "this is not a valid pubsub message"
  expected_invalid_pubsub_dict = {
      'result': {},
      'input': 'this is not a valid pubsub message',
      'success': False
  }

  def test_normal_convert(self):
    with TestPipeline() as p:
      convert_result = (
          p
          | beam.Create([self.valid_pubsub_string])
          | PubsubToQido())
      assert_that(convert_result, equal_to([self.expected_valid_pubsub_dict]))

  def test_failed_convert(self):
    with TestPipeline() as p:
      convert_result = (
          p
          | beam.Create([self.invalid_pubsub_string])
          | PubsubToQido())
      assert_that(convert_result, equal_to([self.expected_invalid_pubsub_dict]))


class TestDicomSearch(unittest.TestCase):
  @patch("dicomio.DicomApiHttpClient")
  def test_successful_search(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"
    input_dict['dataset_id'] = "test_dataset_id"
    input_dict['dicom_store_id'] = "test_dicom_store_id"
    input_dict['search_type'] = "instances"

    fc = FakeHttpClient()
    FakeClient.return_value = fc

    expected_dict = {}
    expected_dict['result'] = fc.dicom_metadata
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_dict]))

  @patch("dicomio.DicomApiHttpClient")
  def test_param_dict_passing(self, FakeClient):
    input_dict = {}
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"
    input_dict['dataset_id'] = "test_dataset_id"
    input_dict['dicom_store_id'] = "test_dicom_store_id"
    input_dict['search_type'] = "instances"
    input_dict['params'] = {'PatientName': 'Brian'}

    expected_dict = {}
    expected_dict['result'] = [{
        'PatientName': 'Brian', 'Age': 20, 'TestResult': 'Positive'
    }]
    expected_dict['status'] = 200
    expected_dict['input'] = input_dict
    expected_dict['success'] = True

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_dict]))

  @patch("dicomio.DicomApiHttpClient")
  def test_wrong_input_type(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"
    input_dict['dataset_id'] = "test_dataset_id"
    input_dict['dicom_store_id'] = "test_dicom_store_id"
    input_dict['search_type'] = "not exist type"

    expected_invalid_dict = {}
    expected_invalid_dict['result'] = []
    expected_invalid_dict[
        'status'] = 'Search type can only be "studies", "instances" or "series"'
    expected_invalid_dict['input'] = input_dict
    expected_invalid_dict['success'] = False

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_invalid_dict]))

  @patch("dicomio.DicomApiHttpClient")
  def test_missing_parameters(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"

    expected_invalid_dict = {}
    expected_invalid_dict['result'] = []
    expected_invalid_dict['status'] = 'Must have dataset_id in the dict.'
    expected_invalid_dict['input'] = input_dict
    expected_invalid_dict['success'] = False

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_invalid_dict]))

  @patch("dicomio.DicomApiHttpClient")
  def test_client_search_notfound(self, FakeClient):
    input_dict = {}
    # search instances in a not exist store
    input_dict['project_id'] = "wrong_project"
    input_dict['region'] = "wrong_region"
    input_dict['dataset_id'] = "wrong_dataset_id"
    input_dict['dicom_store_id'] = "wrong_dicom_store_id"
    input_dict['search_type'] = "instances"

    expected_invalid_dict = {}
    expected_invalid_dict['result'] = []
    expected_invalid_dict['status'] = 204
    expected_invalid_dict['input'] = input_dict
    expected_invalid_dict['success'] = False

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with TestPipeline() as p:
      results = (p | beam.Create([input_dict]) | DicomSearch())
      assert_that(results, equal_to([expected_invalid_dict]))


class TestDicomStoreInstance(_TestCaseWithTempDirCleanUp):
  @patch("dicomio.DicomApiHttpClient")
  def test_store_byte_file(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"
    input_dict['dataset_id'] = "test_dataset_id"
    input_dict['dicom_store_id'] = "test_dicom_store_id"

    fc = FakeHttpClient()
    FakeClient.return_value = fc

    dict_input = {'PatientName': 'George', 'Age': 23, 'TestResult': 'Negative'}
    str_input = json.dumps(dict_input)
    bytes_input = bytes(str_input.encode("utf-8"))
    with TestPipeline() as p:
      results = (
          p
          | beam.Create([bytes_input])
          | DicomStoreInstance(input_dict, 'bytes')
          | beam.Map(lambda x: x['success']))
      assert_that(results, equal_to([True]))
    self.assertTrue(dict_input in fc.dicom_metadata)

  @patch("dicomio.DicomApiHttpClient")
  def test_store_fileio_file(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"
    input_dict['dataset_id'] = "test_dataset_id"
    input_dict['dicom_store_id'] = "test_dicom_store_id"

    fc = FakeHttpClient()
    FakeClient.return_value = fc

    dict_input = {'PatientName': 'George', 'Age': 23, 'TestResult': 'Negative'}
    str_input = json.dumps(dict_input)
    temp_dir = '%s%s' % (self._new_tempdir(), os.sep)
    self._create_temp_file(dir=temp_dir, content=str_input)

    with TestPipeline() as p:
      results = (
          p
          | beam.Create([FileSystems.join(temp_dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | DicomStoreInstance(input_dict, 'fileio')
          | beam.Map(lambda x: x['success']))
      assert_that(results, equal_to([True]))
    self.assertTrue(dict_input in fc.dicom_metadata)

  @patch("dicomio.DicomApiHttpClient")
  def test_destination_notfound(self, FakeClient):
    input_dict = {}
    # search instances in a not exist store
    input_dict['project_id'] = "wrong_project"
    input_dict['region'] = "wrong_region"
    input_dict['dataset_id'] = "wrong_dataset_id"
    input_dict['dicom_store_id'] = "wrong_dicom_store_id"

    expected_invalid_dict = {}
    expected_invalid_dict['status'] = 204
    expected_invalid_dict['input'] = ''
    expected_invalid_dict['success'] = False

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with TestPipeline() as p:
      results = (
          p
          | beam.Create([''])
          | DicomStoreInstance(input_dict, 'bytes'))
      assert_that(results, equal_to([expected_invalid_dict]))

  @patch("dicomio.DicomApiHttpClient")
  def test_missing_parameters(self, FakeClient):
    input_dict = {}
    input_dict['project_id'] = "test_project"
    input_dict['region'] = "test_region"

    expected_invalid_dict = {}
    expected_invalid_dict['result'] = []
    expected_invalid_dict['status'] = 'Must have dataset_id in the dict.'
    expected_invalid_dict['input'] = input_dict
    expected_invalid_dict['success'] = False

    fc = FakeHttpClient()
    FakeClient.return_value = fc
    with self.assertRaisesRegex(ValueError,
                                "Must have dataset_id in the dict."):
      p = TestPipeline()
      _ = (p | beam.Create(['']) | DicomStoreInstance(input_dict, 'bytes'))


if __name__ == '__main__':
  unittest.main()
