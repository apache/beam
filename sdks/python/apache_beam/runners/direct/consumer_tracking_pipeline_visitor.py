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

"""ConsumerTrackingPipelineVisitor, a PipelineVisitor object."""

# pytype: skip-file

from __future__ import absolute_import

from typing import TYPE_CHECKING
from typing import Dict
from typing import Set

from apache_beam import pvalue
from apache_beam.pipeline import PipelineVisitor

if TYPE_CHECKING:
  from apache_beam.pipeline import AppliedPTransform


class ConsumerTrackingPipelineVisitor(PipelineVisitor):
  """For internal use only; no backwards-compatibility guarantees.

  Visitor for extracting value-consumer relations from the graph.

  Tracks the AppliedPTransforms that consume each PValue in the Pipeline. This
  is used to schedule consuming PTransforms to consume input after the upstream
  transform has produced and committed output.
  """
  def __init__(self):
    self.value_to_consumers = {
    }  # type: Dict[pvalue.PValue, Set[AppliedPTransform]]
    self.root_transforms = set()  # type: Set[AppliedPTransform]
    self.step_names = {}  # type: Dict[AppliedPTransform, str]

    self._num_transforms = 0
    self._views = set()

  @property
  def views(self):
    """Returns a list of side intputs extracted from the graph.

    Returns:
      A list of pvalue.AsSideInput.
    """
    return list(self._views)

  def visit_transform(self, applied_ptransform):
    # type: (AppliedPTransform) -> None
    inputs = list(applied_ptransform.inputs)
    if inputs:
      for input_value in inputs:
        if isinstance(input_value, pvalue.PBegin):
          self.root_transforms.add(applied_ptransform)
        if input_value not in self.value_to_consumers:
          self.value_to_consumers[input_value] = set()
        self.value_to_consumers[input_value].add(applied_ptransform)
    else:
      self.root_transforms.add(applied_ptransform)
    self.step_names[applied_ptransform] = 's%d' % (self._num_transforms)
    self._num_transforms += 1

    for side_input in applied_ptransform.side_inputs:
      self._views.add(side_input)
