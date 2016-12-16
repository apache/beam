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

"""End-to-end test result verifiers

A set of verifiers that are used in end-to-end tests to verify state/output
of test pipeline job. Customized verifier should extend
hamcrest.core.base_matcher.BaseMatcher and override _matches.
"""

from apache_beam.runners.runner import PipelineState
from hamcrest.core.base_matcher import BaseMatcher


class PipelineStateMatcher(BaseMatcher):
  """Matcher that verify pipeline job terminated in success"""

  def _matches(self, item):
    return item.current_state() == PipelineState.DONE

  def describe_to(self, description):
    description \
      .append_text("Test pipeline expected terminated in state: ") \
      .append_text(PipelineState.DONE)

  def describe_mismatch(self, item, mismatch_description):
    mismatch_description \
      .append_text("Test pipeline job terminated in state: ") \
      .append_text(item.current_state())
