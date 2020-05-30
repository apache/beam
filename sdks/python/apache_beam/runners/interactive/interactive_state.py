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

from __future__ import absolute_import

import logging
import warnings

import apache_beam as beam
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir

_LOGGER = logging.getLogger(__name__)


class InteractiveState:
  def __init__(self, user_pipeline):
    self.user_pipeline = user_pipeline
    self._pipeline_instrument = pi.PipelineInstrument(self.user_pipeline)
    self._background_caching_job = None
    self._test_stream_service = None
    self._result = None

  @property
  def result(self):
    pass

  @result.setter
  def result(self, value):
    pass

  @property
  def background_caching_job(self):
    pass

  @background_caching_job.setter
  def background_caching_job(self, value):
    pass

  @property
  def test_stream_service(self):
    pass

  @test_stream_service.setter
  def test_stream_service(self, value):
    pass
