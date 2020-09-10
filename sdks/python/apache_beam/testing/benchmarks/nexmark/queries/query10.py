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
Query 10, 'Log to sharded files' (Not in original suite.)

Every window_size_sec, save all events from the last period into
2*max_workers log files.
"""

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration

NUM_SHARD_PER_WORKER = 5
LATE_BATCHING_PERIOD = Duration.of(10)

output_path = None
max_num_workers = 5

num_log_shards = NUM_SHARD_PER_WORKER * max_num_workers


class OutputFile(object):
  def __init__(self, max_timestamp, shard, index, timing, filename):
    self.max_timestamp = max_timestamp
    self.shard = shard
    self.index = index
    self.timing = timing
    self.filename = filename


def open_writable_gcs_file(options, filename):
  # TODO: it seems that beam team has not yet decided about this method and
  #   it is left blank and unspecified.
  pass


def output_file_for(window, shard, pane):
  # type: (window.BoundedWindow, str, beam.DoFn.PaneInfoParam) -> OutputFile
  filename = '%s/LOG-%s-%s-%03d-%s' % (
      output_path, window.max_timestamp(), shard, pane.index,
      pane.timing) if output_path else None
  return OutputFile(
      window.max_timestamp(), shard, pane.index, pane.timing, filename)


def index_path_for(window):
  # type: (window.BoundedWindow) -> [str, None]
  if output_path:
    return '%s/INDEX-%s' % (output_path, window.max_timestamp())
  else:
    return None


def load(events, pipeline_options, metadata=None):
  return (
      events
      | 'query10_shard_events' >> beam.ParDo(ShardEventsDoFn())
      | 'query10_fix_window' >> beam.WindowInto(
          window.FixedWindows(metadata.get('window_size_sec')),
          trigger=trigger.AfterEach(
              trigger.OrFinally(
                  trigger.Repeatedly(
                      trigger.AfterCount(metadata.get('max_log_events'))),
                  trigger.AfterWatermark()),
              trigger.Repeatedly(
                  trigger.AfterAny(
                      trigger.AfterCount(metadata.get('max_log_events')),
                      trigger.AfterProcessingTime(LATE_BATCHING_PERIOD)))),
          accumulation_mode=trigger.AccumulationMode.DISCARDING,
          # Use a 1 day allowed lateness so that any forgotten hold will stall
          # the pipeline for that period and be very noticeable.
          allowed_lateness=Duration.of(1 * 24 * 60 * 60))
      | 'query10_gbk' >> beam.GroupByKey()
      | 'query10_write_event' >> beam.ParDo(WriteEventDoFn(), pipeline_options)
      | 'query10_window_log_files' >> beam.WindowInto(
          window.FixedWindows(metadata.get('window_size_sec')),
          accumulation_mode=trigger.AccumulationMode.DISCARDING,
          allowed_lateness=Duration.of(1 * 24 * 60 * 60))
      | 'query10_gbk_2' >> beam.GroupByKey()
      | 'query10_write_index' >> beam.ParDo(WriteIndexDoFn(), pipeline_options))


class ShardEventsDoFn(beam.DoFn):
  def process(self, element):
    shard_number = abs(hash(element) % num_log_shards)
    shard = 'shard-%05d-of-%05d' % (shard_number, num_log_shards)
    yield shard, element


class WriteEventDoFn(beam.DoFn):
  def process(
      self,
      element,
      pipeline_options,
      window=beam.DoFn.WindowParam,
      pane_info=beam.DoFn.PaneInfoParam):
    shard = element[0]
    options = pipeline_options.view_as(GoogleCloudOptions)
    output_file = output_file_for(window, shard, pane_info)
    if output_file.filename:
      # not do anything because open_writable_gcs_file does not do anything
      open_writable_gcs_file(options, output_file.filename)
      for event in element[1]:  # pylint: disable=unused-variable
        # write to file
        pass
    yield None, output_file


class WriteIndexDoFn(beam.DoFn):
  def process(self, element, pipeline_options, window=beam.DoFn.WindowParam):
    options = pipeline_options.view_as(GoogleCloudOptions)
    filename = index_path_for(window)
    if filename:
      # not do anything because open_writable_gcs_file does not do anything
      open_writable_gcs_file(options, filename)
      for output_file in element[1]:  # pylint: disable=unused-variable
        # write to file
        pass
