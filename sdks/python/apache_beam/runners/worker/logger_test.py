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

"""Tests for worker logging utilities."""

from __future__ import absolute_import
from __future__ import unicode_literals

import json
import logging
import sys
import threading
import unittest
from builtins import object

from apache_beam.runners.worker import logger
from apache_beam.runners.worker import statesampler
from apache_beam.utils.counters import CounterFactory


class PerThreadLoggingContextTest(unittest.TestCase):

  def thread_check_attribute(self, name):
    self.assertFalse(name in logger.per_thread_worker_data.get_data())
    with logger.PerThreadLoggingContext(**{name: 'thread-value'}):
      self.assertEqual(
          logger.per_thread_worker_data.get_data()[name], 'thread-value')
    self.assertFalse(name in logger.per_thread_worker_data.get_data())

  def test_per_thread_attribute(self):
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())
    with logger.PerThreadLoggingContext(xyz='value'):
      self.assertEqual(logger.per_thread_worker_data.get_data()['xyz'], 'value')
      thread = threading.Thread(
          target=self.thread_check_attribute, args=('xyz',))
      thread.start()
      thread.join()
      self.assertEqual(logger.per_thread_worker_data.get_data()['xyz'], 'value')
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())

  def test_set_when_undefined(self):
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())
    with logger.PerThreadLoggingContext(xyz='value'):
      self.assertEqual(logger.per_thread_worker_data.get_data()['xyz'], 'value')
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())

  def test_set_when_already_defined(self):
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())
    with logger.PerThreadLoggingContext(xyz='value'):
      self.assertEqual(logger.per_thread_worker_data.get_data()['xyz'], 'value')
      with logger.PerThreadLoggingContext(xyz='value2'):
        self.assertEqual(
            logger.per_thread_worker_data.get_data()['xyz'], 'value2')
      self.assertEqual(logger.per_thread_worker_data.get_data()['xyz'], 'value')
    self.assertFalse('xyz' in logger.per_thread_worker_data.get_data())


class JsonLogFormatterTest(unittest.TestCase):

  SAMPLE_RECORD = {
      'created': 123456.789, 'msecs': 789.654321,
      'msg': '%s:%d:%.2f', 'args': ('xyz', 4, 3.14),
      'levelname': 'WARNING',
      'process': 'pid', 'thread': 'tid',
      'name': 'name', 'filename': 'file', 'funcName': 'func',
      'exc_info': None}

  SAMPLE_OUTPUT = {
      'timestamp': {'seconds': 123456, 'nanos': 789654321},
      'severity': 'WARN', 'message': 'xyz:4:3.14', 'thread': 'pid:tid',
      'job': 'jobid', 'worker': 'workerid', 'logger': 'name:file:func'}

  def create_log_record(self, **kwargs):

    class Record(object):

      def __init__(self, **kwargs):
        for k, v in kwargs.items():
          setattr(self, k, v)

    return Record(**kwargs)

  def test_basic_record(self):
    formatter = logger.JsonLogFormatter(job_id='jobid', worker_id='workerid')
    record = self.create_log_record(**self.SAMPLE_RECORD)
    self.assertEqual(json.loads(formatter.format(record)), self.SAMPLE_OUTPUT)

  def execute_multiple_cases(self, test_cases):
    record = self.SAMPLE_RECORD
    output = self.SAMPLE_OUTPUT
    formatter = logger.JsonLogFormatter(job_id='jobid', worker_id='workerid')

    for case in test_cases:
      record['msg'] = case['msg']
      record['args'] = case['args']
      output['message'] = case['expected']

      self.assertEqual(
          json.loads(formatter.format(self.create_log_record(**record))),
          output)

  def test_record_with_format_character(self):
    test_cases = [
        {'msg': '%A', 'args': (), 'expected': '%A'},
        {'msg': '%s', 'args': (), 'expected': '%s'},
        {'msg': '%A%s', 'args': ('xy'), 'expected': '%A%s with args (xy)'},
        {'msg': '%s%s', 'args': (1), 'expected': '%s%s with args (1)'},
    ]

    self.execute_multiple_cases(test_cases)

  def test_record_with_arbitrary_messages(self):
    test_cases = [
        {'msg': ImportError('abc'), 'args': (), 'expected': 'abc'},
        {'msg': TypeError('abc %s'), 'args': ('def'), 'expected': 'abc def'},
    ]

    self.execute_multiple_cases(test_cases)

  def test_record_with_per_thread_info(self):
    self.maxDiff = None
    tracker = statesampler.StateSampler('stage', CounterFactory())
    statesampler.set_current_tracker(tracker)
    formatter = logger.JsonLogFormatter(job_id='jobid', worker_id='workerid')
    with logger.PerThreadLoggingContext(work_item_id='workitem'):
      with tracker.scoped_state('step', 'process'):
        record = self.create_log_record(**self.SAMPLE_RECORD)
        log_output = json.loads(formatter.format(record))
    expected_output = dict(self.SAMPLE_OUTPUT)
    expected_output.update(
        {'work': 'workitem', 'stage': 'stage', 'step': 'step'})
    self.assertEqual(log_output, expected_output)
    statesampler.set_current_tracker(None)

  def test_nested_with_per_thread_info(self):
    self.maxDiff = None
    tracker = statesampler.StateSampler('stage', CounterFactory())
    statesampler.set_current_tracker(tracker)
    formatter = logger.JsonLogFormatter(job_id='jobid', worker_id='workerid')
    with logger.PerThreadLoggingContext(work_item_id='workitem'):
      with tracker.scoped_state('step1', 'process'):
        record = self.create_log_record(**self.SAMPLE_RECORD)
        log_output1 = json.loads(formatter.format(record))

        with tracker.scoped_state('step2', 'process'):
          record = self.create_log_record(**self.SAMPLE_RECORD)
          log_output2 = json.loads(formatter.format(record))

        record = self.create_log_record(**self.SAMPLE_RECORD)
        log_output3 = json.loads(formatter.format(record))

    statesampler.set_current_tracker(None)
    record = self.create_log_record(**self.SAMPLE_RECORD)
    log_output4 = json.loads(formatter.format(record))

    self.assertEqual(log_output1, dict(
        self.SAMPLE_OUTPUT, work='workitem', stage='stage', step='step1'))
    self.assertEqual(log_output2, dict(
        self.SAMPLE_OUTPUT, work='workitem', stage='stage', step='step2'))
    self.assertEqual(log_output3, dict(
        self.SAMPLE_OUTPUT, work='workitem', stage='stage', step='step1'))
    self.assertEqual(log_output4, self.SAMPLE_OUTPUT)

  def test_exception_record(self):
    formatter = logger.JsonLogFormatter(job_id='jobid', worker_id='workerid')
    try:
      raise ValueError('Something')
    except ValueError:
      attribs = dict(self.SAMPLE_RECORD)
      attribs.update({'exc_info': sys.exc_info()})
      record = self.create_log_record(**attribs)
    log_output = json.loads(formatter.format(record))
    # Check if exception type, its message, and stack trace information are in.
    exn_output = log_output.pop('exception')
    self.assertNotEqual(exn_output.find('ValueError: Something'), -1)
    self.assertNotEqual(exn_output.find('logger_test.py'), -1)
    self.assertEqual(log_output, self.SAMPLE_OUTPUT)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
