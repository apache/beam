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

"""Nexmark launcher.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. The launcher orchestrates the generation
and parsing of streaming events and the running of queries.

Model
  - Person: Author of an auction or a bid.
  - Auction: Item under auction.
  - Bid: A bid for an item under auction.

Events
 - Create Person
 - Create Auction
 - Create Bid

Queries
  - Query0: Pass through (send and receive auction events).

Usage
  - DirectRunner
      python nexmark_launcher.py \
          --query/q <query number> \
          --project <project id> \
          --loglevel=DEBUG (optional) \
          --wait_until_finish_duration <time_in_ms> \
          --streaming

  - DataflowRunner
      python nexmark_launcher.py \
          --query/q <query number> \
          --project <project id> \
          --region <GCE region> \
          --loglevel=DEBUG (optional) \
          --wait_until_finish_duration <time_in_ms> \
          --streaming \
          --sdk_location <apache_beam tar.gz> \
          --staging_location=gs://... \
          --temp_location=gs://

"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from apache_beam.testing.benchmarks.nexmark import nexmark_util
from apache_beam.testing.benchmarks.nexmark.monitor import Monitor
from apache_beam.testing.benchmarks.nexmark.monitor import MonitorSuffix
from apache_beam.testing.benchmarks.nexmark.nexmark_perf import NexmarkPerf
from apache_beam.testing.benchmarks.nexmark.queries import query0
from apache_beam.testing.benchmarks.nexmark.queries import query1
from apache_beam.testing.benchmarks.nexmark.queries import query2
from apache_beam.testing.benchmarks.nexmark.queries import query3
from apache_beam.testing.benchmarks.nexmark.queries import query4
from apache_beam.testing.benchmarks.nexmark.queries import query5
from apache_beam.testing.benchmarks.nexmark.queries import query6
from apache_beam.testing.benchmarks.nexmark.queries import query7
from apache_beam.testing.benchmarks.nexmark.queries import query8
from apache_beam.testing.benchmarks.nexmark.queries import query9
from apache_beam.testing.benchmarks.nexmark.queries import query11
from apache_beam.testing.benchmarks.nexmark.queries import query12
from apache_beam.transforms import window


class NexmarkLauncher(object):

  # how long after some result is seen and no activity seen do we cancel job
  DONE_DELAY = 5 * 60
  # delay in seconds between sample perf data
  PERF_DELAY = 20
  # delay before cancelling the job when pipeline appears to be stuck
  TERMINATE_DELAY = 1 * 60 * 60
  # delay before warning when pipeline appears to be stuck
  WARNING_DELAY = 10 * 60

  def __init__(self):
    self.parse_args()
    self.manage_resources = self.args.manage_resources
    self.uuid = str(uuid.uuid4()) if self.manage_resources else ''
    self.topic_name = (
        self.args.topic_name + self.uuid if self.args.topic_name else None)
    self.subscription_name = (
        self.args.subscription_name +
        self.uuid if self.args.subscription_name else None)
    self.pubsub_mode = self.args.pubsub_mode
    if self.manage_resources:
      from google.cloud import pubsub
      self.cleanup()
      publish_client = pubsub.Client(project=self.project)
      topic = publish_client.topic(self.topic_name)
      logging.info('creating topic %s', self.topic_name)
      topic.create()
      sub = topic.subscription(self.subscription_name)
      logging.info('creating sub %s', self.topic_name)
      sub.create()

  def parse_args(self):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--query',
        '-q',
        type=int,
        action='append',
        required=True,
        choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12],
        help='Query to run')

    parser.add_argument(
        '--subscription_name',
        type=str,
        help='Pub/Sub subscription to read from')

    parser.add_argument(
        '--topic_name', type=str, help='Pub/Sub topic to read from')

    parser.add_argument(
        '--loglevel',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level to debug')
    parser.add_argument(
        '--input',
        type=str,
        help='Path to the data file containing nexmark events.')
    parser.add_argument(
        '--num_events',
        type=int,
        default=100000,
        help='number of events expected to process')
    parser.add_argument(
        '--manage_resources',
        default=False,
        action='store_true',
        help='If true, manage the creation and cleanup of topics and '
        'subscriptions.')
    parser.add_argument(
        '--pubsub_mode',
        type=str,
        default='SUBSCRIBE_ONLY',
        choices=['PUBLISH_ONLY', 'SUBSCRIBE_ONLY', 'COMBINED'],
        help='Pubsub mode used in the pipeline.')

    self.args, self.pipeline_args = parser.parse_known_args()
    logging.basicConfig(
        level=getattr(logging, self.args.loglevel, None),
        format='(%(threadName)-10s) %(message)s')

    self.pipeline_options = PipelineOptions(self.pipeline_args)
    logging.debug('args, pipeline_args: %s, %s', self.args, self.pipeline_args)

    # Usage with Dataflow requires a project to be supplied.
    self.project = self.pipeline_options.view_as(GoogleCloudOptions).project
    self.streaming = self.pipeline_options.view_as(StandardOptions).streaming

    if self.streaming:
      if self.args.subscription_name is None or self.project is None:
        raise ValueError(
            'argument --subscription_name and --project ' +
            'are required when running in streaming mode')
    else:
      if self.args.input is None:
        raise ValueError(
            'argument --input is required when running in batch mode')

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    self.pipeline_options.view_as(SetupOptions).save_main_session = True

  def generate_events(self):
    from google.cloud import pubsub
    publish_client = pubsub.Client(project=self.project)
    topic = publish_client.topic(self.topic_name)

    logging.info('Generating auction events to topic %s', topic.name)

    if self.args.input.startswith('gs://'):
      from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
      fs = GCSFileSystem(self.pipeline_options)
      with fs.open(self.args.input) as infile:
        for line in infile:
          topic.publish(line)
    else:
      with open(self.args.input) as infile:
        for line in infile:
          topic.publish(line)

    logging.info('Finished event generation.')

  def read_from_file(self):
    return (
        self.pipeline
        | 'reading_from_file' >> beam.io.ReadFromText(self.args.input)
        | 'deserialization' >> beam.ParDo(nexmark_util.ParseJsonEventFn())
        | 'timestamping' >>
        beam.Map(lambda e: window.TimestampedValue(e, e.date_time)))

  def read_from_pubsub(self):
    # Read from PubSub into a PCollection.
    if self.subscription_name:
      raw_events = self.pipeline | 'ReadPubSub_sub' >> beam.io.ReadFromPubSub(
          subscription=self.subscription_name,
          with_attributes=True,
          timestamp_attribute='timestamp')
    else:
      raw_events = self.pipeline | 'ReadPubSub_topic' >> beam.io.ReadFromPubSub(
          topic=self.topic_name,
          with_attributes=True,
          timestamp_attribute='timestamp')
    events = (
        raw_events
        | 'pubsub_unwrap' >> beam.Map(lambda m: m.data)
        | 'deserialization' >> beam.ParDo(nexmark_util.ParseJsonEventFn()))
    return events

  def run_query(self, query, query_args, query_errors):
    try:
      self.pipeline = beam.Pipeline(options=self.pipeline_options)
      nexmark_util.setup_coder()

      event_monitor = Monitor('.events', 'event')
      result_monitor = Monitor('.results', 'result')

      if self.streaming:
        if self.pubsub_mode != 'SUBSCRIBE_ONLY':
          self.generate_events()
        if self.pubsub_mode == 'PUBLISH_ONLY':
          return
        events = self.read_from_pubsub()
      else:
        events = self.read_from_file()

      events = events | 'event_monitor' >> beam.ParDo(event_monitor.doFn)
      output = query.load(events, query_args)
      output | 'result_monitor' >> beam.ParDo(result_monitor.doFn)  # pylint: disable=expression-not-assigned

      result = self.pipeline.run()
      if not self.streaming:
        result.wait_until_finish()
      perf = self.monitor(result, event_monitor, result_monitor)
      self.log_performance(perf)

    except Exception as exc:
      query_errors.append(str(exc))
      raise

  def monitor(self, job, event_monitor, result_monitor):
    """
    keep monitoring the performance and progress of running job and cancel
    the job if the job is stuck or seems to have finished running

    Returns:
      the final performance if it is measured
    """
    logging.info('starting to monitor the job')
    last_active_ms = -1
    perf = None
    cancel_job = False
    waiting_for_shutdown = False

    while True:
      now = int(time.time() * 1000)  # current time in ms
      logging.debug('now is %d', now)

      curr_perf = NexmarkLauncher.get_performance(
          job, event_monitor, result_monitor)
      if perf is None or curr_perf.has_progress(perf):
        last_active_ms = now

      # only judge if the job should be cancelled if it is streaming job and
      # has not been shut down already
      if self.streaming and not waiting_for_shutdown:
        quiet_duration = (now - last_active_ms) // 1000
        if (curr_perf.event_count >= self.args.num_events and
            curr_perf.result_count >= 0 and quiet_duration > self.DONE_DELAY):
          # we think the job is finished if expected input count has been seen
          # and no new results have been produced for a while
          logging.info('streaming query appears to have finished executing')
          waiting_for_shutdown = True
          cancel_job = True
        elif quiet_duration > self.TERMINATE_DELAY:
          logging.error(
              'streaming query have been stuck for %d seconds', quiet_duration)
          logging.error('canceling streaming job')
          waiting_for_shutdown = True
          cancel_job = True
        elif quiet_duration > self.WARNING_DELAY:
          logging.warning(
              'streaming query have been stuck for %d seconds', quiet_duration)

        if cancel_job:
          job.cancel()

      perf = curr_perf

      stopped = PipelineState.is_terminal(job.state)
      if stopped:
        break

      if not waiting_for_shutdown:
        if last_active_ms == now:
          logging.info('activity seen, new performance data extracted')
        else:
          logging.info('no activity seen')
      else:
        logging.info('waiting for shutdown')

      time.sleep(self.PERF_DELAY)

    return perf

  @staticmethod
  def log_performance(perf: NexmarkPerf) -> None:
    logging.info(
        'input event count: %d, output event count: %d' %
        (perf.event_count, perf.result_count))
    logging.info(
        'query run took %.1f seconds and processed %.1f events per second' %
        (perf.runtime_sec, perf.event_per_sec))

  @staticmethod
  def get_performance(result, event_monitor, result_monitor):
    event_count = nexmark_util.get_counter_metric(
        result,
        event_monitor.namespace,
        event_monitor.name_prefix + MonitorSuffix.ELEMENT_COUNTER)
    event_start = nexmark_util.get_start_time_metric(
        result,
        event_monitor.namespace,
        event_monitor.name_prefix + MonitorSuffix.EVENT_TIME)
    event_end = nexmark_util.get_end_time_metric(
        result,
        event_monitor.namespace,
        event_monitor.name_prefix + MonitorSuffix.EVENT_TIME)
    result_count = nexmark_util.get_counter_metric(
        result,
        result_monitor.namespace,
        result_monitor.name_prefix + MonitorSuffix.ELEMENT_COUNTER)
    result_end = nexmark_util.get_end_time_metric(
        result,
        result_monitor.namespace,
        result_monitor.name_prefix + MonitorSuffix.EVENT_TIME)

    perf = NexmarkPerf()
    perf.event_count = event_count
    perf.result_count = result_count
    effective_end = max(event_end, result_end)
    if effective_end >= 0 and event_start >= 0:
      perf.runtime_sec = (effective_end - event_start) / 1000
    if event_count >= 0 and perf.runtime_sec > 0:
      perf.event_per_sec = event_count / perf.runtime_sec

    return perf

  def cleanup(self):
    if self.manage_resources:
      from google.cloud import pubsub
      publish_client = pubsub.Client(project=self.project)
      topic = publish_client.topic(self.topic_name)
      if topic.exists():
        logging.info('deleting topic %s', self.topic_name)
        topic.delete()
      sub = topic.subscription(self.subscription_name)
      if sub.exists():
        logging.info('deleting sub %s', self.topic_name)
        sub.delete()

  def run(self):
    queries = {
        0: query0,
        1: query1,
        2: query2,
        3: query3,
        4: query4,
        5: query5,
        6: query6,
        7: query7,
        8: query8,
        9: query9,
        11: query11,
        12: query12
    }

    # TODO(mariagh): Move to a config file.
    query_args = {
        'auction_skip': 123,
        'window_size_sec': 10,
        'window_period_sec': 5,
        'fanout': 5,
        'num_max_workers': 5,
        'max_log_events': 100000,
        'occasional_delay_sec': 3,
        'max_auction_waiting_time': 600
    }

    query_errors = []
    for i in self.args.query:
      logging.info('Running query %d', i)
      self.run_query(queries[i], query_args, query_errors=query_errors)

    if query_errors:
      logging.error('Query failed with %s', ', '.join(query_errors))
    else:
      logging.info('Queries run: %s', self.args.query)


if __name__ == '__main__':
  launcher = NexmarkLauncher()
  launcher.run()
  launcher.cleanup()
