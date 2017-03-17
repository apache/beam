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

from __future__ import absolute_import

from apache_beam import pvalue
from apache_beam.pipeline import PipelineVisitor


class ConsumerTrackingPipelineVisitor(PipelineVisitor):
  """Visitor for extracting value-consumer relations from the graph.

  Tracks the AppliedPTransforms that consume each PValue in the Pipeline. This
  is used to schedule consuming PTransforms to consume input after the upstream
  transform has produced and committed output.
  """

  def __init__(self):
    self.value_to_consumers = {}  # Map from PValue to [AppliedPTransform].
    self.root_transforms = set()  # set of (root) AppliedPTransforms.
    self.views = []               # list of PCollectionViews.
    self.step_names = {}          # Map from AppliedPTransform to String.

    self._num_transforms = 0

  def visit_value(self, value, producer_node):
    if value:
      if isinstance(value, pvalue.PCollectionView):
        self.views.append(value)

  def visit_transform(self, applied_ptransform):
    inputs = applied_ptransform.inputs
    if inputs:
      for input_value in inputs:
        if isinstance(input_value, pvalue.PBegin):
          self.root_transforms.add(applied_ptransform)
        if input_value not in self.value_to_consumers:
          self.value_to_consumers[input_value] = []
        self.value_to_consumers[input_value].append(applied_ptransform)
    else:
      self.root_transforms.add(applied_ptransform)
    self.step_names[applied_ptransform] = 's%d' % (self._num_transforms)
    self._num_transforms += 1
