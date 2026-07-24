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

"""Integration tests for the cross-language JMS IO transforms
(ReadFromJms / WriteToJms), served by the messaging expansion service.

Runs against ActiveMQ or IBM MQ brokers started once per test class via testcontainers.
"""

import logging
import platform
import re
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
  from apache_beam.io import ReadFromJms
  from apache_beam.io import WriteToJms
except ImportError:
  ReadFromJms = None
  WriteToJms = None

try:
  from testcontainers.core.container import DockerContainer
  from testcontainers.core.waiting_utils import wait_for_logs
except ImportError:
  DockerContainer = None

_LOGGER = logging.getLogger(__name__)
NUM_RECORDS = 100
STRING_ROW = RowTypeConstraint.from_fields([('payload', str)])


@pytest.mark.uses_messaging_java_expansion_service
@unittest.skipIf(
    DockerContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    ReadFromJms is None or WriteToJms is None,
    'JMS cross-language wrappers are not generated')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    'Do not run this test on precommit suites.')
@unittest.skipIf(
    'Dataflow' in (
        TestPipeline().get_pipeline_options().view_as(StandardOptions).runner or
        ''),
    'The testcontainers broker is not reachable from Dataflow workers; '
    'a Dataflow variant would need a remotely hosted JMS broker.')
class _BaseJmsIOTest(unittest.TestCase):
  expansion_service = None

  def produce(self, source_queue, count):
    raise NotImplementedError

  def browse_queue(self, sink_queue):
    raise NotImplementedError

  def _connection_configuration(self, connection_param=None):
    raise NotImplementedError

  def _run_streaming_test(
      self,
      source_queue,
      sink_queue,
      acknowledge_mode=None,
      connection_param=None):
    subscriber_result = {}

    def publish():
      self.produce(source_queue, remaining_records)

    stop_event = threading.Event()

    def subscribe():
      received_messages = []
      while len(received_messages) < NUM_RECORDS and not stop_event.is_set():
        time.sleep(5)
        try:
          messages = self.browse_queue(sink_queue)
          if messages:
            received_messages = messages
            subscriber_result['received'] = received_messages
        except Exception as e:
          _LOGGER.warning('Error while browsing sink queue: %s', e)
          break
        _LOGGER.info('received %s messages', len(received_messages))

    # TODO(https://github.com/apache/beam/issues/39446): Clean up
    # pre-publishing Prism runner issue resolved
    initial_records = 10
    remaining_records = NUM_RECORDS - initial_records
    self.produce(source_queue, initial_records)

    publisher = threading.Thread(target=publish, daemon=True)
    subscriber = threading.Thread(target=subscribe, daemon=True)

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
          | 'ReadFromJms' >> ReadFromJms(
              connection_configuration=self._connection_configuration(
                  connection_param),
              queue=source_queue,
              acknowledge_mode=acknowledge_mode,
              expansion_service=self.expansion_service)
          |
          'Passthrough' >> beam.Map(lambda row: beam.Row(payload=row.payload)
                                    ).with_output_types(STRING_ROW)
          | 'WriteToJms' >> WriteToJms(
              connection_configuration=self._connection_configuration(
                  connection_param),
              queue=sink_queue,
              expansion_service=self.expansion_service))
      publisher.start()
      result = p.run()
      subscriber.start()
      try:
        subscriber.join(timeout=20)  # 1.5 min
      finally:
        stop_event.set()
        publisher.join()
        try:
          result.cancel()
        except Exception:  # pylint: disable=broad-except
          _LOGGER.warning('Ignoring error while cancelling the pipeline.')
    finally:
      p._extra_context.__exit__(None, None, None)

    received = subscriber_result.get('received', [])
    self.assertEqual(len(received), NUM_RECORDS)
    # there are identical records
    self.assertEqual(len(set(received)), NUM_RECORDS - initial_records)


class ActiveMQJmsIOTest(_BaseJmsIOTest):
  @classmethod
  def setUpClass(cls):
    cls.start_jms_container(retries=3)
    host = cls.broker.get_container_host_ip()
    port = cls.broker.get_exposed_port(61616)
    cls.server_uri = 'tcp://%s:%s' % (host, port)

  @classmethod
  def tearDownClass(cls):
    try:
      cls.broker.stop()
    except Exception:
      logging.error('Could not stop the JMS broker container.')

  @classmethod
  def start_jms_container(cls, retries):
    for i in range(retries):
      try:
        cls.broker = DockerContainer(
            'apache/activemq-classic:5.18.3').with_exposed_ports(61616)
        cls.broker.start()
        wait_for_logs(cls.broker, '.*ActiveMQ .* started.*', timeout=30)
        break
      except Exception as e:
        try:
          cls.broker.stop()
        except Exception:
          pass
        if i == retries - 1:
          logging.error('Unable to initialize the JMS broker container.')
          raise e

  def _connection_configuration(self, connection_param=None):
    uri = self.server_uri
    if connection_param:
      uri += '?' + connection_param
    return {
        'server_uri': uri,
        'connection_factory_class_name': 'org.apache.activemq.ActiveMQConnectionFactory'
    }

  def produce(self, source_queue, count):
    container = self.broker.get_wrapped_container()
    exit_code, _ = container.exec_run([
        '/opt/apache-activemq/bin/activemq',
        'producer',
        '--destination',
        'queue://' + source_queue,
        '--messageCount',
        str(count),
        '--persistent',
        'false'
    ])
    if exit_code == 0:
      _LOGGER.info('published %s messages to %s', count, source_queue)
    else:
      _LOGGER.warning('publishing message returns exit code %s', exit_code)

  def browse_queue(self, sink_queue):
    container = self.broker.get_wrapped_container()
    exit_code, output = container.exec_run([
        '/opt/apache-activemq/bin/activemq',
        'browse',
        sink_queue
    ])
    if exit_code == 0 and output:
      return [
          line.split('JMS_BODY_FIELD:JMSText = ')[-1].strip()
          for line in output.decode('utf-8').splitlines()
          if 'JMS_BODY_FIELD:JMSText = ' in line
      ]
    return []

  def test_xlang_jms_write_read_queue_ind_ack(self):
    self._run_streaming_test(
        source_queue='xlang-jms-ind-source',
        sink_queue='xlang-jms-ind-sink',
        acknowledge_mode='INDIVIDUAL_ACKNOWLEDGE')

  def test_xlang_jms_write_read_queue(self):
    self._run_streaming_test(
        source_queue='xlang-jms-source',
        sink_queue='xlang-jms-sink',
        acknowledge_mode='CLIENT_ACKNOWLEDGE_UNSAFE',
        connection_param='jms.prefetchPolicy.all=0')


class IbmMqJmsIOTest(_BaseJmsIOTest):
  @classmethod
  def setUpClass(cls):
    cls.start_ibm_mq_container(retries=3)

  @classmethod
  def tearDownClass(cls):
    if getattr(cls, 'expansion_service_obj', None):
      try:
        cls.expansion_service_obj.__exit__(None, None, None)
      except Exception:
        pass
    if getattr(cls, 'broker', None):
      try:
        cls.broker.stop()
      except Exception:
        logging.error('Could not stop the IBM MQ broker container.')

  @classmethod
  def get_ibm_mq_image(cls):
    arch = platform.machine().lower()
    if 'arm' in arch or 'aarch64' in arch:
      try:
        import docker
        client = docker.from_env()
        for img in client.images.list():
          for tag in img.tags:
            if ('ibm-mq' in tag.lower() or
                'ibm_mq' in tag.lower()) and 'arm64' in tag.lower():
              _LOGGER.info('Found local ARM64 IBM MQ image: %s', tag)
              return tag
      except Exception as e:
        _LOGGER.warning('Failed to inspect local docker images: %s', e)

      raise RuntimeError(
          'Official IBM MQ docker images do not support ARM macOS (aarch64). '
          'Please build an ARM64 image locally from '
          'https://github.com/ibm-messaging/mq-container '
          'and tag it with an "-arm64" suffix (e.g. localhost/ibm-mqadvanced-server-dev:9.4.0.0-arm64).'
      )
    else:
      return 'icr.io/ibm-messaging/mq:9.3.0.25-r1'

  @classmethod
  def start_ibm_mq_container(cls, retries):
    from apache_beam.transforms.external import BeamJarExpansionService
    image_tag = cls.get_ibm_mq_image()
    for i in range(retries):
      try:
        cls.broker = DockerContainer(image_tag).with_env(
            'LICENSE', 'accept').with_env('MQ_QMGR_NAME', 'QM1').with_env(
                'MQ_APP_PASSWORD', 'admin123').with_exposed_ports(1414)
        cls.broker.start()
        wait_for_logs(
            cls.broker, '.*(MQQMNAME|Started queue manager).*', timeout=45)
        host = cls.broker.get_container_host_ip()
        port = cls.broker.get_exposed_port(1414)
        cls.server_uri = 'tcp://%s:%s' % (host, port)
        cls.expansion_service_obj = BeamJarExpansionService(
            'sdks:java:io:messaging-expansion-service:shadowJar',
            classpath=[
                'com.ibm.mq:com.ibm.mq.allclient:9.3.0.25',
                'org.json:json:20251224'
            ])
        cls.expansion_service = cls.expansion_service_obj.__enter__()
        break
      except Exception as e:
        if getattr(cls, 'broker', None):
          try:
            cls.broker.stop()
          except Exception:
            pass
        if i == retries - 1:
          logging.error('Unable to initialize the IBM MQ broker container.')
          raise e

  def _connection_configuration(self, connection_param=None):
    uri = self.server_uri + '?channel=DEV.APP.SVRCONN&queueManager=QM1'
    if connection_param:
      uri += '&' + connection_param
    return {
        'server_uri': uri,
        'connection_factory_class_name': 'com.ibm.mq.jms.MQConnectionFactory',
        'username': 'app',
        'password': 'admin123'
    }

  def produce(self, source_queue, count):
    container = self.broker.get_wrapped_container()
    cmd = (
        f'for i in $(seq 0 {count-1}); do '
        f'echo "test message: $i" | /opt/mqm/samp/bin/amqsput {source_queue} QM1 >/dev/null 2>&1; '
        f'done')
    container.exec_run(['sh', '-c', cmd])

  def browse_queue(self, sink_queue):
    container = self.broker.get_wrapped_container()
    exit_code, output = container.exec_run([
        '/opt/mqm/bin/dmpmqmsg',
        '-m',
        'QM1',
        '-i',
        sink_queue,
        '-f',
        'stdout',
        '-d',
        'p'
    ])

    if exit_code == 0 and output:
      content = output.decode('utf-8', errors='ignore')
      messages = []
      current_msg = []
      for line in content.splitlines():
        # Example raw result:
        # S "9749158</Tms><Dlv>2</Dlv></jms>  test message: 7"
        # S "8"
        if 'test message:' in line:
          current_msg.append(line[1:].strip('"'))
        elif re.match(r'^S "\d+"$', line):
          current_msg.append(line[2:].strip('"') + " ")
      if current_msg:
        full_str = "".join(current_msg)
        # extract all "test message: [number]" from the full string
        messages = re.findall(r'test message: \d+', full_str)
        return messages
    return []

  def test_xlang_jms_write_read_queue_client_ack(self):
    self._run_streaming_test(
        source_queue='DEV.QUEUE.1',
        sink_queue='DEV.QUEUE.2',
        acknowledge_mode='CLIENT_ACKNOWLEDGE')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
