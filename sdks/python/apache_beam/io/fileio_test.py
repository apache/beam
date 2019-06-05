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

"""Tests for transforms defined in apache_beam.io.fileio."""

from __future__ import absolute_import

import csv
import io
import logging
import sys
import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import compute_hash
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class MatchTest(_TestCaseWithTempDirCleanUp):

  def test_basic_two_files(self):
    files = []
    tempdir = '%s/' % self._new_tempdir()

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir))
    files.append(self._create_temp_file(dir=tempdir))

    with TestPipeline() as p:
      files_pc = p | fileio.MatchFiles(tempdir) | beam.Map(lambda x: x.path)

      assert_that(files_pc, equal_to(files))

  def test_match_all_two_directories(self):
    files = []
    directories = []

    for _ in range(2):
      # TODO: What about this having to append the ending slash?
      d = '%s/' % self._new_tempdir()
      directories.append(d)

      files.append(self._create_temp_file(dir=d))
      files.append(self._create_temp_file(dir=d))

    with TestPipeline() as p:
      files_pc = (p
                  | beam.Create(directories)
                  | fileio.MatchAll()
                  | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure(self):
    directories = [
        '%s/' % self._new_tempdir(),
        '%s/' % self._new_tempdir()]

    files = list()
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        files_pc = (
            p
            | beam.Create(directories)
            | fileio.MatchAll(fileio.EmptyMatchTreatment.DISALLOW)
            | beam.Map(lambda x: x.path))

        assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure(self):
    directories = [
        '%s/' % self._new_tempdir(),
        '%s/' % self._new_tempdir()]

    files = list()
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create(['%s*' % d for d in directories])
          | fileio.MatchAll(fileio.EmptyMatchTreatment.ALLOW_IF_WILDCARD)
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))


class ReadTest(_TestCaseWithTempDirCleanUp):

  def test_basic_file_name_provided(self):
    content = 'TestingMyContent\nIn multiple lines\nhaha!'
    dir = '%s/' % self._new_tempdir()
    self._create_temp_file(dir=dir, content=content)

    with TestPipeline() as p:
      content_pc = (p
                    | beam.Create([dir])
                    | fileio.MatchAll()
                    | fileio.ReadMatches()
                    | beam.Map(lambda f: f.read().decode('utf-8')))

      assert_that(content_pc, equal_to([content]))

  def test_csv_file_source(self):
    content = 'name,year,place\ngoogle,1999,CA\nspotify,2006,sweden'
    rows = [r.split(',') for r in content.split('\n')]

    dir = '%s/' % self._new_tempdir()
    self._create_temp_file(dir=dir, content=content)

    def get_csv_reader(readable_file):
      if sys.version_info >= (3, 0):
        return csv.reader(io.TextIOWrapper(readable_file.open()))
      else:
        return csv.reader(readable_file.open())

    with TestPipeline() as p:
      content_pc = (p
                    | beam.Create([dir])
                    | fileio.MatchAll()
                    | fileio.ReadMatches()
                    | beam.FlatMap(get_csv_reader))

      assert_that(content_pc, equal_to(rows))

  def test_string_filenames_and_skip_directory(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s/' % self._new_tempdir()

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with TestPipeline() as p:
      contents_pc = (p
                     | beam.Create(files + [tempdir])
                     | fileio.ReadMatches()
                     | beam.Map(lambda x: x.read().decode('utf-8')))

      assert_that(contents_pc, equal_to([content]*2))

  def test_fail_on_directories(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s/' % self._new_tempdir()

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        _ = (p
             | beam.Create(files + [tempdir])
             | fileio.ReadMatches(skip_directories=False)
             | beam.Map(lambda x: x.read_utf8()))


class MatchIntegrationTest(unittest.TestCase):

  INPUT_FILE = 'gs://dataflow-samples/shakespeare/kinglear.txt'
  KINGLEAR_CHECKSUM = 'f418b25f1507f5a901257026b035ac2857a7ab87'
  INPUT_FILE_LARGE = (
      'gs://dataflow-samples/wikipedia_edits/wiki_data-00000000000*.json')

  WIKI_FILES = [
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000000.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000001.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000002.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000003.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000004.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000005.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000006.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000007.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000008.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000009.json',
  ]

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

  @attr('IT')
  def test_transform_on_gcs(self):
    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      matches_pc = (p
                    | beam.Create([self.INPUT_FILE, self.INPUT_FILE_LARGE])
                    | fileio.MatchAll()
                    | 'GetPath' >> beam.Map(lambda metadata: metadata.path))

      assert_that(matches_pc,
                  equal_to([self.INPUT_FILE] + self.WIKI_FILES),
                  label='Matched Files')

      checksum_pc = (p
                     | 'SingleFile' >> beam.Create([self.INPUT_FILE])
                     | 'MatchOneAll' >> fileio.MatchAll()
                     | fileio.ReadMatches()
                     | 'ReadIn' >> beam.Map(lambda x: x.read_utf8().split('\n'))
                     | 'Checksums' >> beam.Map(compute_hash))

      assert_that(checksum_pc,
                  equal_to([self.KINGLEAR_CHECKSUM]),
                  label='Assert Checksums')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
