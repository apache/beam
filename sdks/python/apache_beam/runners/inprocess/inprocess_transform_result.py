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

"""The result of evaluating an AppliedPTransform with a TransformEvaluator."""

from __future__ import absolute_import


class InProcessTransformResult(object):
  """The result of evaluating an AppliedPTransform with a TransformEvaluator."""

  def __init__(self, applied_ptransform, uncommitted_output_bundles, state,
               timer_update, counters, watermark_hold,
               undeclared_tag_values=None):
    self._applied_ptransform = applied_ptransform
    self._uncommitted_output_bundles = uncommitted_output_bundles
    self._state = state
    self._timer_update = timer_update
    self._counters = counters
    self._watermark_hold = watermark_hold
    # Only used when caching (materializing) all values is requested.
    self._undeclared_tag_values = undeclared_tag_values

  @property
  def transform(self):
    return self._applied_ptransform

  @property
  def output_bundles(self):
    return self._uncommitted_output_bundles

  @property
  def state(self):
    return self._state

  @property
  def counters(self):
    return self._counters

  @property
  def watermark_hold(self):
    return self._watermark_hold

  @property
  def undeclared_tag_values(self):
    return self._undeclared_tag_values
