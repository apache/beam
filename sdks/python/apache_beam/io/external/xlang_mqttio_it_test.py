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

"""Integration tests for the cross-language MQTT IO transforms
(ReadFromMqtt / WriteToMqtt), served by the messaging expansion service.

Runs against an MQTT broker (Eclipse Mosquitto) started once per test class
via testcontainers. MqttIO reads are unbounded (streaming), so the end-to-end
read/write test runs on the Prism portable streaming runner -- the legacy
DirectRunner cannot execute an unbounded read (see the
MqttReadSchemaTransformProvider description).
"""

import logging
import threading
import time
import unittest

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.typehints.row_type import RowTypeConstraint

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.io import ReadFromMqtt
  from apache_beam.io import WriteToMqtt
except ImportError:
  ReadFromMqtt = None
  WriteToMqtt = None

try:
  from testcontainers.core.container import DockerContainer
  from testcontainers.core.waiting_utils import wait_for_logs
except ImportError:
  DockerContainer = None

NUM_RECORDS = 3
BYTES_ROW = RowTypeConstraint.from_fields([('bytes', bytes)])


@pytest.mark.uses_messaging_java_expansion_service
@unittest.skipIf(
    DockerContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    ReadFromMqtt is None or WriteToMqtt is None,
    'MQTT cross-language wrappers are not generated')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    'Do not run this test on precommit suites.')
@unittest.skipIf(
    'Dataflow' in (
        TestPipeline().get_pipeline_options().view_as(StandardOptions).runner or
        ''),
    'The testcontainers broker is not reachable from Dataflow workers; '
    'a Dataflow variant would need a remotely hosted MQTT broker.')
class CrossLanguageMqttIOTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # The broker is expensive to spin up and tear down, so start a single
    # shared instance for the whole class; each test uses its own topic(s).
    cls.start_mqtt_container(retries=3)
    host = cls.broker.get_container_host_ip()
    port = cls.broker.get_exposed_port(1883)
    cls.server_uri = 'tcp://%s:%s' % (host, port)

  @classmethod
  def tearDownClass(cls):
    # Sometimes stopping the container raises ReadTimeout. We can ignore it
    # here to avoid the test failure.
    try:
      cls.broker.stop()
    except Exception:
      logging.error('Could not stop the MQTT broker container.')

  # Creating a container with testcontainers sometimes raises ReadTimeout
  # error, so retry a couple of times.
  @classmethod
  def start_mqtt_container(cls, retries):
    for i in range(retries):
      try:
        # /mosquitto-no-auth.conf ships with the image and enables an
        # anonymous listener on port 1883.
        cls.broker = DockerContainer('eclipse-mosquitto:2').with_command(
            'mosquitto -c /mosquitto-no-auth.conf').with_exposed_ports(1883)
        cls.broker.start()
        wait_for_logs(cls.broker, 'mosquitto version .* running', timeout=30)
        break
      except Exception as e:
        # If start() succeeded but a later step (e.g. wait_for_logs) failed,
        # stop the partially started container so the next retry / the raised
        # error does not leak a running Docker container.
        try:
          cls.broker.stop()
        except Exception:
          pass
        if i == retries - 1:
          logging.error('Unable to initialize the MQTT broker container.')
          raise e

  def _connection_configuration(self, topic, client_id):
    return {
        'server_uri': self.server_uri, 'topic': topic, 'client_id': client_id
    }

  def test_xlang_mqtt_write(self):
    topic = 'xlang-mqtt-write-topic'
    expected_payloads = [b'msg-%d' % i for i in range(NUM_RECORDS)]
    subscriber_result = {}

    def subscribe():
      # mosquitto_sub exits after receiving NUM_RECORDS messages (-C) or
      # after the timeout (-W), printing one payload per line.
      container = self.broker.get_wrapped_container()
      exit_code, output = container.exec_run([
          'mosquitto_sub',
          '-t',
          topic,
          '-q',
          '1',
          '-C',
          str(NUM_RECORDS),
          '-W',
          '120'
      ])
      subscriber_result['exit_code'] = exit_code
      subscriber_result['output'] = output

    subscriber = threading.Thread(target=subscribe, daemon=True)
    subscriber.start()
    # Give the subscriber time to connect before publishing.
    time.sleep(5)

    with TestPipeline() as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | 'CreatePayloads' >> beam.Create(expected_payloads)
          | 'ToRow' >> beam.Map(lambda payload: beam.Row(bytes=payload)).
          with_output_types(BYTES_ROW)
          | 'WriteToMqtt' >> WriteToMqtt(
              connection_configuration=self._connection_configuration(
                  topic, 'xlang-mqtt-write')))

    subscriber.join(timeout=150)
    self.assertEqual(subscriber_result.get('exit_code'), 0)
    received = sorted(subscriber_result.get('output', b'').split())
    self.assertEqual(sorted(expected_payloads), received)

  def test_xlang_mqtt_read_write_streaming(self):
    """Exercises ReadFromMqtt and WriteToMqtt end to end on the Prism portable
    streaming runner. MqttIO read is unbounded, which the legacy DirectRunner
    cannot execute, so this is the single read test: an unbounded ReadFromMqtt
    on a source topic feeds a WriteToMqtt on a sink topic, the result is
    observed with a mosquitto_sub subscriber on the sink topic, and the
    (never-terminating) pipeline is then cancelled.

    MQTT does not retain regular messages, so the reader must already be
    subscribed when messages are published -- a Kafka-style sequential
    write-then-read would read nothing. A background publisher therefore feeds
    the source topic continuously while the streaming pipeline runs.
    """
    source_topic = 'xlang-mqtt-streaming-source'
    sink_topic = 'xlang-mqtt-streaming-sink'
    stop_publishing = threading.Event()
    subscriber_result = {}

    def publish_loop():
      container = self.broker.get_wrapped_container()
      i = 0
      while not stop_publishing.is_set():
        container.exec_run([
            'mosquitto_pub', '-t', source_topic, '-m', 'msg-%d' % i, '-q', '1'
        ])
        i += 1
        time.sleep(0.5)

    def subscribe():
      container = self.broker.get_wrapped_container()
      exit_code, output = container.exec_run([
          'mosquitto_sub',
          '-t',
          sink_topic,
          '-q',
          '1',
          '-C',
          str(NUM_RECORDS),
          '-W',
          '180'
      ])
      subscriber_result['exit_code'] = exit_code
      subscriber_result['output'] = output

    publisher = threading.Thread(target=publish_loop, daemon=True)
    subscriber = threading.Thread(target=subscribe, daemon=True)
    publisher.start()
    subscriber.start()

    p = TestPipeline(blocking=False)
    p.get_pipeline_options().view_as(StandardOptions).streaming = True
    p.not_use_test_runner_api = True
    # Run pipeline without blocking
    # TODO: Remove once subprocess cache leak fixed for pipeline running
    # in LOOPBACK mode outside of with clause
    p.__enter__()
    try:
      _ = (
          p
          | 'ReadFromMqtt' >> ReadFromMqtt(
              connection_configuration=self._connection_configuration(
                  source_topic, 'xlang-mqtt-streaming-read'))
          | 'Passthrough' >> beam.Map(lambda row: beam.Row(bytes=row.bytes)).
          with_output_types(BYTES_ROW)
          | 'WriteToMqtt' >> WriteToMqtt(
              connection_configuration=self._connection_configuration(
                  sink_topic, 'xlang-mqtt-streaming-write')))
      result = p.run()
      try:
        # The subscriber exits once NUM_RECORDS messages flowed through the
        # streaming pipeline (or fails the assertions below on its timeout).
        subscriber.join(timeout=200)
      finally:
        stop_publishing.set()
        publisher.join()
        try:
          result.cancel()
        except Exception:  # pylint: disable=broad-except
          # The unbounded pipeline never finishes on its own; cancellation
          # after the assertion data was collected is best-effort.
          logging.warning('Ignoring error while cancelling the pipeline.')
    finally:
      p._extra_context.__exit__(None, None, None)

    self.assertEqual(subscriber_result.get('exit_code'), 0)
    payloads = subscriber_result.get('output', b'').split()
    self.assertEqual(NUM_RECORDS, len(payloads))
    for payload in payloads:
      self.assertTrue(
          payload.startswith(b'msg-'), 'Unexpected payload: %s' % payload)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
