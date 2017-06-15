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


class TransformResult(object):
  """For internal use only; no backwards-compatibility guarantees.

  The result of evaluating an AppliedPTransform with a TransformEvaluator."""

  def __init__(self, applied_ptransform, uncommitted_output_bundles,
               timer_update, counters, watermark_hold,
               undeclared_tag_values=None):
    self.transform = applied_ptransform
    self.uncommitted_output_bundles = uncommitted_output_bundles
    # TODO: timer update is currently unused.
    self.timer_update = timer_update
    self.counters = counters
    self.watermark_hold = watermark_hold
    # Only used when caching (materializing) all values is requested.
    self.undeclared_tag_values = undeclared_tag_values
    # Populated by the TransformExecutor.
    self.logical_metric_updates = None
