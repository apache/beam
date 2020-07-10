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

"""Module to control how Interactive Beam captures data from sources for
deterministic replayable PCollection evaluation and pipeline runs.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
from datetime import timedelta

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.options import capture_limiters

_LOGGER = logging.getLogger(__name__)


class CaptureControl(object):
  """Options and their utilities that controls how Interactive Beam captures
  deterministic replayable data from sources."""
  def __init__(self):
    self._enable_capture_replay = True
    self._capturable_sources = {
        ReadFromPubSub,
    }  # yapf: disable
    self._capture_duration = timedelta(seconds=60)
    self._capture_size_limit = 1e9
    self._test_limiters = None

  def limiters(self):
    # type: () -> List[capture_limiters.Limiter]
    if self._test_limiters:
      return self._test_limiters
    return [
        capture_limiters.SizeLimiter(self._capture_size_limit),
        capture_limiters.DurationLimiter(self._capture_duration)
    ]

  def set_limiters_for_test(self, limiters):
    # type: (List[capture_limiters.Limiter]) -> None
    self._test_limiters = limiters


def evict_captured_data(pipeline=None):
  """Evicts all deterministic replayable data that have been captured by
  Interactive Beam for the given pipeline. If no pipeline is specified, evicts
  for all user defined pipelines.

  In future PCollection evaluation/visualization and pipeline
  runs, Interactive Beam will capture fresh data."""
  if ie.current_env().options.enable_capture_replay:
    _LOGGER.info(
        'You have requested Interactive Beam to evict all captured '
        'data that could be deterministically replayed among multiple '
        'pipeline runs.')
  ie.current_env().track_user_pipelines()
  if pipeline:
    bcj.attempt_to_cancel_background_caching_job(pipeline)
    bcj.attempt_to_stop_test_stream_service(pipeline)
  else:
    for user_pipeline in ie.current_env().tracked_user_pipelines:
      bcj.attempt_to_cancel_background_caching_job(user_pipeline)
      bcj.attempt_to_stop_test_stream_service(user_pipeline)
  ie.current_env().cleanup(pipeline)
