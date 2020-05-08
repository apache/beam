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

"""UnitTests for DoFn lifecycle and bundle methods"""

# pytype: skip-file

from __future__ import absolute_import

import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline


class CallSequenceEnforcingDoFn(beam.DoFn):
  def __init__(self):
    self._setup_called = False
    self._start_bundle_calls = 0
    self._finish_bundle_calls = 0
    self._teardown_called = False

  def setup(self):
    assert not self._setup_called, 'setup should not be called twice'
    assert self._start_bundle_calls == 0, \
      'setup should be called before start_bundle'
    assert self._finish_bundle_calls == 0, \
      'setup should be called before finish_bundle'
    assert not self._teardown_called, 'setup should be called before teardown'
    self._setup_called = True

  def start_bundle(self):
    assert self._setup_called, 'setup should have been called'
    assert self._start_bundle_calls == self._finish_bundle_calls, \
      'there should be as many start_bundle calls as finish_bundle calls'
    assert not self._teardown_called, 'teardown should not have been called'
    self._start_bundle_calls += 1

  def process(self, element):
    assert self._setup_called, 'setup should have been called'
    assert self._start_bundle_calls > 0, 'start_bundle should have been called'
    assert self._start_bundle_calls == self._finish_bundle_calls + 1, \
      'there should be one start_bundle call with no call to finish_bundle'
    assert not self._teardown_called, 'teardown should not have been called'
    return [element * element]

  def finish_bundle(self):
    assert self._setup_called, 'setup should have been called'
    assert self._start_bundle_calls > 0, 'start_bundle should have been called'
    assert self._start_bundle_calls == self._finish_bundle_calls + 1, \
      'there should be one start_bundle call with no call to finish_bundle'
    assert not self._teardown_called, 'teardown should not have been called'
    self._finish_bundle_calls += 1

  def teardown(self):
    assert self._setup_called, 'setup should have been called'
    assert self._start_bundle_calls == self._finish_bundle_calls, \
      'there should be as many start_bundle calls as finish_bundle calls'
    assert not self._teardown_called, 'teardown should not be called twice'
    self._teardown_called = True


@attr('ValidatesRunner')
class DoFnLifecycleTest(unittest.TestCase):
  def test_dofn_lifecycle(self):
    with TestPipeline() as p:
      _ = (
          p
          | 'Start' >> beam.Create([1, 2, 3])
          | 'Do' >> beam.ParDo(CallSequenceEnforcingDoFn()))
    # Assumes that the worker is run in the same process as the test.


class LocalDoFnLifecycleTest(unittest.TestCase):
  def test_dofn_lifecycle(self):
    from apache_beam.runners.direct import direct_runner
    from apache_beam.runners.portability import fn_api_runner
    runners = [
        direct_runner.BundleBasedDirectRunner(), fn_api_runner.FnApiRunner()
    ]
    for r in runners:
      with TestPipeline(runner=r) as p:
        _ = (
            p
            | 'Start' >> beam.Create([1, 2, 3])
            | 'Do' >> beam.ParDo(CallSequenceEnforcingDoFn()))
      # Assumes that the worker is run in the same process as the test.


if __name__ == '__main__':
  unittest.main()
