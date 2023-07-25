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

# pytype: skip-file

import csv
import io
import json
import logging
import os
import unittest
import uuid
import warnings

import pytest
from hamcrest.library.text import stringmatches

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_utils import compute_hash
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import matches_all
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp

warnings.filterwarnings(
    'ignore', category=FutureWarning, module='apache_beam.io.fileio_test')


def _get_file_reader(readable_file):
  return io.TextIOWrapper(readable_file.open())


class MatchTest(_TestCaseWithTempDirCleanUp):
  def test_basic_two_files(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir))
    files.append(self._create_temp_file(dir=tempdir))

    with TestPipeline() as p:
      files_pc = (
          p
          | fileio.MatchFiles(FileSystems.join(tempdir, '*'))
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))

  def test_match_all_two_directories(self):
    files = []
    directories = []

    for _ in range(2):
      # TODO: What about this having to append the ending slash?
      d = '%s%s' % (self._new_tempdir(), os.sep)
      directories.append(d)

      files.append(self._create_temp_file(dir=d))
      files.append(self._create_temp_file(dir=d))

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create([FileSystems.join(d, '*') for d in directories])
          | fileio.MatchAll()
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure1(self):
    directories = [
        '%s%s' % (self._new_tempdir(), os.sep),
        '%s%s' % (self._new_tempdir(), os.sep)
    ]

    files = []
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        files_pc = (
            p
            | beam.Create([FileSystems.join(d, '*') for d in directories])
            | fileio.MatchAll(fileio.EmptyMatchTreatment.DISALLOW)
            | beam.Map(lambda x: x.path))

        assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure2(self):
    directories = [
        '%s%s' % (self._new_tempdir(), os.sep),
        '%s%s' % (self._new_tempdir(), os.sep)
    ]

    files = []
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create([FileSystems.join(d, '*') for d in directories])
          | fileio.MatchAll(fileio.EmptyMatchTreatment.ALLOW_IF_WILDCARD)
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))


class ReadTest(_TestCaseWithTempDirCleanUp):
  def test_basic_file_name_provided(self):
    content = 'TestingMyContent\nIn multiple lines\nhaha!'
    dir = '%s%s' % (self._new_tempdir(), os.sep)
    self._create_temp_file(dir=dir, content=content)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read().decode('utf-8').splitlines()))

      assert_that(content_pc, equal_to(content.splitlines()))

  def test_csv_file_source(self):
    content = 'name,year,place\ngoogle,1999,CA\nspotify,2006,sweden'
    rows = [r.split(',') for r in content.split('\n')]

    dir = '%s%s' % (self._new_tempdir(), os.sep)
    self._create_temp_file(dir=dir, content=content)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.FlatMap(lambda rf: csv.reader(_get_file_reader(rf))))

      assert_that(content_pc, equal_to(rows))

  def test_infer_compressed_file(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import gzip
    with gzip.GzipFile(os.path.join(dir, 'compressed.gz'), 'w') as f:
      f.write(file_contents)

    file_contents2 = b'compressed_contents_bz2!'
    import bz2
    with bz2.BZ2File(os.path.join(dir, 'compressed2.bz2'), 'w') as f:
      f.write(file_contents2)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(lambda rf: rf.open().readline()))

      assert_that(content_pc, equal_to([file_contents, file_contents2]))

  def test_read_bz2_compressed_file_without_suffix(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import bz2
    with bz2.BZ2File(os.path.join(dir, 'compressed'), 'w') as f:
      f.write(file_contents)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(
              lambda rf: rf.open(compression_type=CompressionTypes.BZIP2).read(
                  len(file_contents))))

      assert_that(content_pc, equal_to([file_contents]))

  def test_read_gzip_compressed_file_without_suffix(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import gzip
    with gzip.GzipFile(os.path.join(dir, 'compressed'), 'w') as f:
      f.write(file_contents)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(
              lambda rf: rf.open(compression_type=CompressionTypes.GZIP).read(
                  len(file_contents))))

      assert_that(content_pc, equal_to([file_contents]))

  def test_string_filenames_and_skip_directory(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with TestPipeline() as p:
      contents_pc = (
          p
          | beam.Create(files + ['%s/' % tempdir])
          | fileio.ReadMatches()
          | beam.FlatMap(lambda x: x.read().decode('utf-8').splitlines()))

      assert_that(contents_pc, equal_to(content.splitlines() * 2))

  def test_fail_on_directories(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    # Create a couple files to be matched
    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        _ = (
            p
            | beam.Create(files + ['%s/' % tempdir])
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

  @pytest.mark.it_postcommit
  def test_transform_on_gcs(self):
    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      matches_pc = (
          p
          | beam.Create([self.INPUT_FILE, self.INPUT_FILE_LARGE])
          | fileio.MatchAll()
          | 'GetPath' >> beam.Map(lambda metadata: metadata.path))

      assert_that(
          matches_pc,
          equal_to([self.INPUT_FILE] + self.WIKI_FILES),
          label='Matched Files')

      checksum_pc = (
          p
          | 'SingleFile' >> beam.Create([self.INPUT_FILE])
          | 'MatchOneAll' >> fileio.MatchAll()
          | fileio.ReadMatches()
          | 'ReadIn' >> beam.Map(lambda x: x.read_utf8().split('\n'))
          | 'Checksums' >> beam.Map(compute_hash))

      assert_that(
          checksum_pc,
          equal_to([self.KINGLEAR_CHECKSUM]),
          label='Assert Checksums')


class MatchContinuouslyTest(_TestCaseWithTempDirCleanUp):
  def test_with_deduplication(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    # Create a file to be matched before pipeline
    files.append(self._create_temp_file(dir=tempdir))
    # Add file name that will be created mid-pipeline
    files.append(FileSystems.join(tempdir, 'extra'))

    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              start_timestamp=start,
              stop_timestamp=stop)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))

  def test_without_deduplication(self):
    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    # Create a file to be matched before pipeline starts
    file = self._create_temp_file(dir=tempdir)
    # Add file twice, since it will be matched for every interval
    files += [file, file]
    # Add file name that will be created mid-pipeline
    files.append(FileSystems.join(tempdir, 'extra'))

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              has_deduplication=False,
              start_timestamp=start,
              stop_timestamp=stop)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))

  def test_match_updated_files(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    # Create two files to be matched before pipeline
    files.append(self._create_temp_file(dir=tempdir))
    writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
    writer.close()

    # Add file name that will be created mid-pipeline
    files.append(FileSystems.join(tempdir, 'extra'))
    files.append(FileSystems.join(tempdir, 'extra'))

    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              start_timestamp=start,
              stop_timestamp=stop,
              match_updated_files=True)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))


class WriteFilesTest(_TestCaseWithTempDirCleanUp):

  SIMPLE_COLLECTION = [
      {
          'project': 'beam', 'foundation': 'apache'
      },
      {
          'project': 'prometheus', 'foundation': 'cncf'
      },
      {
          'project': 'flink', 'foundation': 'apache'
      },
      {
          'project': 'grpc', 'foundation': 'cncf'
      },
      {
          'project': 'spark', 'foundation': 'apache'
      },
      {
          'project': 'kubernetes', 'foundation': 'cncf'
      },
      {
          'project': 'spark', 'foundation': 'apache'
      },
      {
          'project': 'knative', 'foundation': 'cncf'
      },
      {
          'project': 'linux', 'foundation': 'linux'
      },
  ]

  LARGER_COLLECTION = ['{:05d}'.format(i) for i in range(200)]

  CSV_HEADERS = ['project', 'foundation']

  SIMPLE_COLLECTION_VALIDATION_SET = {(elm['project'], elm['foundation'])
                                      for elm in SIMPLE_COLLECTION}

  class CsvSink(fileio.TextSink):
    def __init__(self, headers):
      self.headers = headers

    def write(self, record):
      self._fh.write(','.join([record[h] for h in self.headers]).encode('utf8'))
      self._fh.write('\n'.encode('utf8'))

  class JsonSink(fileio.TextSink):
    def write(self, record):
      self._fh.write(json.dumps(record).encode('utf8'))
      self._fh.write('\n'.encode('utf8'))

  def test_write_to_single_file_batch(self):

    dir = self._new_tempdir()

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | "Serialize" >> beam.Map(json.dumps)
          | beam.io.fileio.WriteToFiles(path=dir))

    with TestPipeline() as p:
      result = (
          p
          | fileio.MatchFiles(FileSystems.join(dir, '*'))
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read_utf8().strip().split('\n'))
          | beam.Map(json.loads))

      assert_that(result, equal_to([row for row in self.SIMPLE_COLLECTION]))

  def test_write_to_dynamic_destination(self):

    sink_params = [
        fileio.TextSink, # pass a type signature
        fileio.TextSink() # pass a FileSink object
    ]

    for sink in sink_params:
      dir = self._new_tempdir()

      with TestPipeline() as p:
        _ = (
            p
            | "Create" >> beam.Create(range(100))
            | beam.Map(lambda x: str(x))
            | fileio.WriteToFiles(
                path=dir,
                destination=lambda n: "odd" if int(n) % 2 else "even",
                sink=sink,
                file_naming=fileio.destination_prefix_naming("test")))

      with TestPipeline() as p:
        result = (
            p
            | fileio.MatchFiles(FileSystems.join(dir, '*'))
            | fileio.ReadMatches()
            | beam.Map(
                lambda f: (
                    os.path.basename(f.metadata.path).split('-')[0],
                    sorted(map(int, f.read_utf8().strip().split('\n'))))))

        assert_that(
            result,
            equal_to([('odd', list(range(1, 100, 2))),
                      ('even', list(range(0, 100, 2)))]))

  def test_write_to_different_file_types_some_spilling(self):

    dir = self._new_tempdir()

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | beam.io.fileio.WriteToFiles(
              path=dir,
              destination=lambda record: record['foundation'],
              sink=lambda dest: (
                  WriteFilesTest.CsvSink(WriteFilesTest.CSV_HEADERS)
                  if dest == 'apache' else WriteFilesTest.JsonSink()),
              file_naming=fileio.destination_prefix_naming(),
              max_writers_per_bundle=1))

    with TestPipeline() as p:
      cncf_res = (
          p
          | fileio.MatchFiles(FileSystems.join(dir, 'cncf*'))
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read_utf8().strip().split('\n'))
          | beam.Map(json.loads))

      apache_res = (
          p
          |
          "MatchApache" >> fileio.MatchFiles(FileSystems.join(dir, 'apache*'))
          | "ReadApache" >> fileio.ReadMatches()
          | "MapApache" >>
          beam.FlatMap(lambda rf: csv.reader(_get_file_reader(rf))))

      assert_that(
          cncf_res,
          equal_to([
              row for row in self.SIMPLE_COLLECTION
              if row['foundation'] == 'cncf'
          ]),
          label='verifyCNCF')

      assert_that(
          apache_res,
          equal_to([[row['project'], row['foundation']]
                    for row in self.SIMPLE_COLLECTION
                    if row['foundation'] == 'apache']),
          label='verifyApache')

  @unittest.skip('https://github.com/apache/beam/issues/21269')
  def test_find_orphaned_files(self):
    dir = self._new_tempdir()

    write_transform = beam.io.fileio.WriteToFiles(path=dir)

    def write_orphaned_file(temp_dir, writer_key):
      temp_dir_path = FileSystems.join(dir, temp_dir)

      file_prefix_dir = FileSystems.join(
          temp_dir_path, str(abs(hash(writer_key))))

      file_name = '%s_%s' % (file_prefix_dir, uuid.uuid4())
      with FileSystems.create(file_name) as f:
        f.write(b'Hello y\'all')

      return file_name

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | "Serialize" >> beam.Map(json.dumps)
          | write_transform)

      # Pre-create the temp directory.
      temp_dir_path = FileSystems.mkdirs(
          FileSystems.join(dir, write_transform._temp_directory.get()))
      write_orphaned_file(
          write_transform._temp_directory.get(), (None, GlobalWindow()))
      f2 = write_orphaned_file(
          write_transform._temp_directory.get(), ('other-dest', GlobalWindow()))

    temp_dir_path = FileSystems.join(dir, write_transform._temp_directory.get())
    leftovers = FileSystems.match(['%s%s*' % (temp_dir_path, os.sep)])
    found_files = [m.path for m in leftovers[0].metadata_list]
    self.assertListEqual(found_files, [f2])

  def test_write_to_different_file_types(self):

    dir = self._new_tempdir()

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | beam.io.fileio.WriteToFiles(
              path=dir,
              destination=lambda record: record['foundation'],
              sink=lambda dest: (
                  WriteFilesTest.CsvSink(WriteFilesTest.CSV_HEADERS)
                  if dest == 'apache' else WriteFilesTest.JsonSink()),
              file_naming=fileio.destination_prefix_naming()))

    with TestPipeline() as p:
      cncf_res = (
          p
          | fileio.MatchFiles(FileSystems.join(dir, 'cncf*'))
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read_utf8().strip().split('\n'))
          | beam.Map(json.loads))

      apache_res = (
          p
          |
          "MatchApache" >> fileio.MatchFiles(FileSystems.join(dir, 'apache*'))
          | "ReadApache" >> fileio.ReadMatches()
          | "MapApache" >>
          beam.FlatMap(lambda rf: csv.reader(_get_file_reader(rf))))

      assert_that(
          cncf_res,
          equal_to([
              row for row in self.SIMPLE_COLLECTION
              if row['foundation'] == 'cncf'
          ]),
          label='verifyCNCF')

      assert_that(
          apache_res,
          equal_to([[row['project'], row['foundation']]
                    for row in self.SIMPLE_COLLECTION
                    if row['foundation'] == 'apache']),
          label='verifyApache')

  def record_dofn(self):
    class RecordDoFn(beam.DoFn):
      def process(self, element):
        WriteFilesTest.all_records.append(element)

    return RecordDoFn()

  def test_streaming_complex_timing(self):
    # Use state on the TestCase class, since other references would be pickled
    # into a closure and not have the desired side effects.
    #
    # TODO(https://github.com/apache/beam/issues/18987): Use assert_that after
    # it works for the cases here in streaming mode.
    WriteFilesTest.all_records = []

    dir = '%s%s' % (self._new_tempdir(), os.sep)

    # Setting up the input (TestStream)
    ts = TestStream().advance_watermark_to(0)
    for elm in WriteFilesTest.LARGER_COLLECTION:
      timestamp = int(elm)

      ts.add_elements([('key', '%s' % elm)])
      if timestamp % 5 == 0 and timestamp != 0:
        # TODO(https://github.com/apache/beam/issues/18721): Add many firings
        # per window after getting PaneInfo.
        ts.advance_processing_time(5)
        ts.advance_watermark_to(timestamp)
    ts.advance_watermark_to_infinity()

    def no_colon_file_naming(*args):
      file_name = fileio.destination_prefix_naming()(*args)
      return file_name.replace(':', '_')

    # The pipeline that we are testing
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      res = (
          p
          | ts
          | beam.WindowInto(
              FixedWindows(10),
              trigger=trigger.AfterWatermark(),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.GroupByKey()
          | beam.FlatMap(lambda x: x[1]))
      # Triggering after 5 processing-time seconds, and on the watermark. Also
      # discarding old elements.

      _ = (
          res
          | beam.io.fileio.WriteToFiles(
              path=dir,
              file_naming=no_colon_file_naming,
              max_writers_per_bundle=0)
          | beam.Map(lambda fr: FileSystems.join(dir, fr.file_name))
          | beam.ParDo(self.record_dofn()))

    # Verification pipeline
    with TestPipeline() as p:
      files = (p | beam.io.fileio.MatchFiles(FileSystems.join(dir, '*')))

      file_names = (files | beam.Map(lambda fm: fm.path))

      file_contents = (
          files
          | beam.io.fileio.ReadMatches()
          | beam.Map(
              lambda rf: (rf.metadata.path, rf.read_utf8().strip().split('\n')))
      )

      content = (
          file_contents
          | beam.FlatMap(lambda fc: [ln.strip() for ln in fc[1]]))

      assert_that(
          file_names,
          equal_to(WriteFilesTest.all_records),
          label='AssertFilesMatch')
      assert_that(
          content,
          matches_all(WriteFilesTest.LARGER_COLLECTION),
          label='AssertContentsMatch')

  def test_streaming_different_file_types(self):
    dir = self._new_tempdir()
    input = iter(WriteFilesTest.SIMPLE_COLLECTION)
    ts = (
        TestStream().advance_watermark_to(0).add_elements(
            [next(input), next(input)]).advance_watermark_to(10).add_elements(
                [next(input),
                 next(input)]).advance_watermark_to(20).add_elements([
                     next(input), next(input)
                 ]).advance_watermark_to(30).add_elements([
                     next(input), next(input)
                 ]).advance_watermark_to(40).advance_watermark_to_infinity())

    def no_colon_file_naming(*args):
      file_name = fileio.destination_prefix_naming()(*args)
      return file_name.replace(':', '_')

    with TestPipeline() as p:
      _ = (
          p
          | ts
          | beam.WindowInto(FixedWindows(10))
          | beam.io.fileio.WriteToFiles(
              path=dir,
              destination=lambda record: record['foundation'],
              sink=lambda dest: (
                  WriteFilesTest.CsvSink(WriteFilesTest.CSV_HEADERS)
                  if dest == 'apache' else WriteFilesTest.JsonSink()),
              file_naming=no_colon_file_naming,
              max_writers_per_bundle=0,
          ))

    with TestPipeline() as p:
      cncf_files = (
          p
          | fileio.MatchFiles(FileSystems.join(dir, 'cncf*'))
          | "CncfFileNames" >> beam.Map(lambda fm: fm.path))

      apache_files = (
          p
          |
          "MatchApache" >> fileio.MatchFiles(FileSystems.join(dir, 'apache*'))
          | "ApacheFileNames" >> beam.Map(lambda fm: fm.path))

      assert_that(
          cncf_files,
          matches_all([
              stringmatches.matches_regexp(
                  '.*cncf-1970-01-01T00_00_00-1970-01-01T00_00_10.*'),
              stringmatches.matches_regexp(
                  '.*cncf-1970-01-01T00_00_10-1970-01-01T00_00_20.*'),
              stringmatches.matches_regexp(
                  '.*cncf-1970-01-01T00_00_20-1970-01-01T00_00_30.*'),
              stringmatches.matches_regexp(
                  '.*cncf-1970-01-01T00_00_30-1970-01-01T00_00_40.*')
          ]),
          label='verifyCNCFFiles')

      assert_that(
          apache_files,
          matches_all([
              stringmatches.matches_regexp(
                  '.*apache-1970-01-01T00_00_00-1970-01-01T00_00_10.*'),
              stringmatches.matches_regexp(
                  '.*apache-1970-01-01T00_00_10-1970-01-01T00_00_20.*'),
              stringmatches.matches_regexp(
                  '.*apache-1970-01-01T00_00_20-1970-01-01T00_00_30.*'),
              stringmatches.matches_regexp(
                  '.*apache-1970-01-01T00_00_30-1970-01-01T00_00_40.*')
          ]),
          label='verifyApacheFiles')

  def test_shard_naming(self):
    namer = fileio.default_file_naming(prefix='/path/to/file', suffix='.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, None, None, None, None),
        '/path/to/file.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, 1, 5, None, None),
        '/path/to/file-00001-of-00005.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, 1, 5, 'gz', None),
        '/path/to/file-00001-of-00005.txt.gz')
    self.assertEqual(
        namer(IntervalWindow(0, 100), None, 1, 5, None, None),
        '/path/to/file'
        '-1970-01-01T00:00:00-1970-01-01T00:01:40-00001-of-00005.txt')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
