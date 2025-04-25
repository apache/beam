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

# pytype: skip-file

import math

import apache_beam as beam
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import combiners
from apache_beam.transforms import trigger
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types


@with_input_types(int)
@with_output_types(int)
class CallSequenceEnforcingCombineFn(beam.CombineFn):
  instances: set['CallSequenceEnforcingCombineFn'] = set()

  def __init__(self):
    super().__init__()
    self._setup_called = False
    self._teardown_called = False

  def setup(self, *args, **kwargs):
    assert not self._setup_called, 'setup should not be called twice'
    assert not self._teardown_called, 'setup should be called before teardown'
    # Keep track of instances so that we can check if teardown is called
    # properly after pipeline execution.
    self.instances.add(self)
    self._setup_called = True

  def create_accumulator(self, *args, **kwargs):
    assert self._setup_called, 'setup should have been called'
    assert not self._teardown_called, 'teardown should not have been called'
    return 0

  def add_input(self, mutable_accumulator, element, *args, **kwargs):
    assert self._setup_called, 'setup should have been called'
    assert not self._teardown_called, 'teardown should not have been called'
    mutable_accumulator += element
    return mutable_accumulator

  def add_inputs(self, mutable_accumulator, elements, *args, **kwargs):
    return self.add_input(mutable_accumulator, sum(elements))

  def merge_accumulators(self, accumulators, *args, **kwargs):
    assert self._setup_called, 'setup should have been called'
    assert not self._teardown_called, 'teardown should not have been called'
    return sum(accumulators)

  def extract_output(self, accumulator, *args, **kwargs):
    assert self._setup_called, 'setup should have been called'
    assert not self._teardown_called, 'teardown should not have been called'
    return accumulator

  def teardown(self, *args, **kwargs):
    assert self._setup_called, 'setup should have been called'
    assert not self._teardown_called, 'teardown should not be called twice'
    self._teardown_called = True


@with_input_types(tuple[None, str])
@with_output_types(tuple[int, str])
class IndexAssigningDoFn(beam.DoFn):
  state_param = beam.DoFn.StateParam(
      userstate.CombiningValueStateSpec(
          'index', beam.coders.VarIntCoder(), CallSequenceEnforcingCombineFn()))

  def process(self, element, state=state_param):
    _, value = element
    current_index = state.read()
    yield current_index, value
    state.add(1)


def run_combine(pipeline, input_elements=5, lift_combiners=True):
  # Calculate the expected result, which is the sum of an arithmetic sequence.
  # By default, this is equal to: 0 + 1 + 2 + 3 + 4 = 10
  expected_result = input_elements * (input_elements - 1) / 2

  # Enable runtime type checking in order to cover TypeCheckCombineFn by
  # the test.
  pipeline.get_pipeline_options().view_as(TypeOptions).runtime_type_check = True
  pipeline.get_pipeline_options().view_as(
      TypeOptions).allow_unsafe_triggers = True

  with pipeline as p:
    pcoll = p | 'Start' >> beam.Create(range(input_elements))

    # Certain triggers, such as AfterCount, are incompatible with combiner
    # lifting. We can use that fact to prevent combiners from being lifted.
    if not lift_combiners:
      pcoll |= beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.AfterCount(input_elements),
          accumulation_mode=trigger.AccumulationMode.DISCARDING)

    # Pass an additional 'None' in order to cover _CurriedFn by the test.
    pcoll |= 'Do' >> beam.CombineGlobally(
        combiners.SingleInputTupleCombineFn(
            CallSequenceEnforcingCombineFn(), CallSequenceEnforcingCombineFn()),
        None).with_fanout(fanout=1)
    assert_that(pcoll, equal_to([(expected_result, expected_result)]))


def run_combine_uncopyable_attr(
    pipeline, input_elements=5, lift_combiners=True):
  # Calculate the expected result, which is the sum of an arithmetic sequence.
  # By default, this is equal to: 0 + 1 + 2 + 3 + 4 = 10
  expected_result = input_elements * (input_elements - 1) / 2

  # Enable runtime type checking in order to cover TypeCheckCombineFn by
  # the test.
  pipeline.get_pipeline_options().view_as(TypeOptions).runtime_type_check = True
  pipeline.get_pipeline_options().view_as(
      TypeOptions).allow_unsafe_triggers = True

  with pipeline as p:
    pcoll = p | 'Start' >> beam.Create(range(input_elements))

    # Certain triggers, such as AfterCount, are incompatible with combiner
    # lifting. We can use that fact to prevent combiners from being lifted.
    if not lift_combiners:
      pcoll |= beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.AfterCount(input_elements),
          accumulation_mode=trigger.AccumulationMode.DISCARDING)

    combine_fn = CallSequenceEnforcingCombineFn()
    # Modules are not deep copyable. Ensure fanout falls back to pickling for
    # copying combine_fn.
    combine_fn.module_attribute = math
    pcoll |= 'Do' >> beam.CombineGlobally(combine_fn).with_fanout(fanout=1)

    assert_that(pcoll, equal_to([expected_result]))


def run_pardo(pipeline, input_elements=10):
  with pipeline as p:
    _ = (
        p
        | 'Start' >> beam.Create(('Hello' for _ in range(input_elements)))
        | 'KeyWithNone' >> beam.Map(lambda elem: (None, elem))
        | 'Do' >> beam.ParDo(IndexAssigningDoFn()))
