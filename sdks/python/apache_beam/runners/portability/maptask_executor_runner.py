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

"""Beam runner for testing/profiling worker code directly.
"""

import collections
import logging
import time

import apache_beam as beam
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.options import pipeline_options
from apache_beam.runners import DataflowRunner
from apache_beam.runners.dataflow.internal.dependency import _dependency_file_copy
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.runners.worker import statesampler
from apache_beam.typehints import typehints
from apache_beam.utils import profiler
from apache_beam.utils.counters import CounterFactory

# This module is experimental. No backwards-compatibility guarantees.


class MapTaskExecutorRunner(PipelineRunner):
  """Beam runner translating a pipeline into map tasks that are then executed.

  Primarily intended for testing and profiling the worker code paths.
  """

  def __init__(self):
    self.executors = []

  def has_metrics_support(self):
    """Returns whether this runner supports metrics or not.
    """
    return False

  def run_pipeline(self, pipeline):
    MetricsEnvironment.set_metrics_supported(self.has_metrics_support())
    # List of map tasks  Each map task is a list of
    # (stage_name, operation_specs.WorkerOperation) instructions.
    self.map_tasks = []

    # Map of pvalues to
    # (map_task_index, producer_operation_index, producer_output_index)
    self.outputs = {}

    # Unique mappings of PCollections to strings.
    self.side_input_labels = collections.defaultdict(
        lambda: str(len(self.side_input_labels)))

    # Mapping of map task indices to all map tasks that must preceed them.
    self.dependencies = collections.defaultdict(set)

    # Visit the graph, building up the map_tasks and their metadata.
    super(MapTaskExecutorRunner, self).run_pipeline(pipeline)

    # Now run the tasks in topological order.
    def compute_depth_map(deps):
      memoized = {}

      def compute_depth(x):
        if x not in memoized:
          memoized[x] = 1 + max([-1] + [compute_depth(y) for y in deps[x]])
        return memoized[x]

      return {x: compute_depth(x) for x in deps.keys()}

    map_task_depths = compute_depth_map(self.dependencies)
    ordered_map_tasks = sorted((map_task_depths.get(ix, -1), map_task)
                               for ix, map_task in enumerate(self.map_tasks))

    profile_options = pipeline.options.view_as(
        pipeline_options.ProfilingOptions)
    if profile_options.profile_cpu:
      with profiler.Profile(
          profile_id='worker-runner',
          profile_location=profile_options.profile_location,
          log_results=True, file_copy_fn=_dependency_file_copy):
        self.execute_map_tasks(ordered_map_tasks)
    else:
      self.execute_map_tasks(ordered_map_tasks)

    return WorkerRunnerResult(PipelineState.UNKNOWN)

  def metrics_containers(self):
    return [op.metrics_container
            for ex in self.executors
            for op in ex.operations()]

  def execute_map_tasks(self, ordered_map_tasks):
    tt = time.time()
    for ix, (_, map_task) in enumerate(ordered_map_tasks):
      logging.info('Running %s', map_task)
      t = time.time()
      stage_names, all_operations = zip(*map_task)
      # TODO(robertwb): The DataflowRunner worker receives system step names
      # (e.g. "s3") that are used to label the output msec counters.  We use the
      # operation names here, but this is not the same scheme used by the
      # DataflowRunner; the result is that the output msec counters are named
      # differently.
      system_names = stage_names
      # Create the CounterFactory and StateSampler for this MapTask.
      # TODO(robertwb): Output counters produced here are currently ignored.
      counter_factory = CounterFactory()
      state_sampler = statesampler.StateSampler('%s' % ix, counter_factory)
      map_executor = operations.SimpleMapTaskExecutor(
          operation_specs.MapTask(
              all_operations, 'S%02d' % ix,
              system_names, stage_names, system_names),
          counter_factory,
          state_sampler)
      self.executors.append(map_executor)
      map_executor.execute()
      logging.info(
          'Stage %s finished: %0.3f sec', stage_names[0], time.time() - t)
    logging.info('Total time: %0.3f sec', time.time() - tt)

  def run_Read(self, transform_node):
    self._run_read_from(transform_node, transform_node.transform.source)

  def _run_read_from(self, transform_node, source):
    """Used when this operation is the result of reading source."""
    if not isinstance(source, NativeSource):
      source = iobase.SourceBundle(1.0, source, None, None)
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    read_op = operation_specs.WorkerRead(source, output_coders=[element_coder])
    self.outputs[output] = len(self.map_tasks), 0, 0
    self.map_tasks.append([(transform_node.full_label, read_op)])
    return len(self.map_tasks) - 1

  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    map_task_index, producer_index, output_index = self.outputs[
        transform_node.inputs[0]]

    # If any of this ParDo's side inputs depend on outputs from this map_task,
    # we can't continue growing this map task.
    def is_reachable(leaf, root):
      if leaf == root:
        return True
      else:
        return any(is_reachable(x, root) for x in self.dependencies[leaf])

    if any(is_reachable(self.outputs[side_input.pvalue][0], map_task_index)
           for side_input in transform_node.side_inputs):
      # Start a new map tasks.
      input_element_coder = self._get_coder(transform_node.inputs[0])

      output_buffer = OutputBuffer(input_element_coder)

      fusion_break_write = operation_specs.WorkerInMemoryWrite(
          output_buffer=output_buffer,
          write_windowed_values=True,
          input=(producer_index, output_index),
          output_coders=[input_element_coder])
      self.map_tasks[map_task_index].append(
          (transform_node.full_label + '/Write', fusion_break_write))

      original_map_task_index = map_task_index
      map_task_index, producer_index, output_index = len(self.map_tasks), 0, 0

      fusion_break_read = operation_specs.WorkerRead(
          output_buffer.source_bundle(),
          output_coders=[input_element_coder])
      self.map_tasks.append(
          [(transform_node.full_label + '/Read', fusion_break_read)])

      self.dependencies[map_task_index].add(original_map_task_index)

    def create_side_read(side_input):
      label = self.side_input_labels[side_input]
      output_buffer = self.run_side_write(
          side_input.pvalue, '%s/%s' % (transform_node.full_label, label))
      return operation_specs.WorkerSideInputSource(
          output_buffer.source(), label)

    do_op = operation_specs.WorkerDoFn(  #
        serialized_fn=pickler.dumps(DataflowRunner._pardo_fn_data(
            transform_node,
            lambda side_input: self.side_input_labels[side_input])),
        output_tags=[PropertyNames.OUT] + ['%s_%s' % (PropertyNames.OUT, tag)
                                           for tag in transform.output_tags
                                          ],
        # Same assumption that DataflowRunner has about coders being compatible
        # across outputs.
        output_coders=[element_coder] * (len(transform.output_tags) + 1),
        input=(producer_index, output_index),
        side_inputs=[create_side_read(side_input)
                     for side_input in transform_node.side_inputs])

    producer_index = len(self.map_tasks[map_task_index])
    self.outputs[transform_node.outputs[None]] = (
        map_task_index, producer_index, 0)
    for ix, tag in enumerate(transform.output_tags):
      self.outputs[transform_node.outputs[
          tag]] = map_task_index, producer_index, ix + 1
    self.map_tasks[map_task_index].append((transform_node.full_label, do_op))

    for side_input in transform_node.side_inputs:
      self.dependencies[map_task_index].add(self.outputs[side_input.pvalue][0])

  def run_side_write(self, pcoll, label):
    map_task_index, producer_index, output_index = self.outputs[pcoll]

    windowed_element_coder = self._get_coder(pcoll)
    output_buffer = OutputBuffer(windowed_element_coder)
    write_sideinput_op = operation_specs.WorkerInMemoryWrite(
        output_buffer=output_buffer,
        write_windowed_values=True,
        input=(producer_index, output_index),
        output_coders=[windowed_element_coder])
    self.map_tasks[map_task_index].append(
        (label, write_sideinput_op))
    return output_buffer

  def run__GroupByKeyOnly(self, transform_node):
    map_task_index, producer_index, output_index = self.outputs[
        transform_node.inputs[0]]
    grouped_element_coder = self._get_coder(transform_node.outputs[None],
                                            windowed=False)
    windowed_ungrouped_element_coder = self._get_coder(transform_node.inputs[0])

    output_buffer = GroupingOutputBuffer(grouped_element_coder)
    shuffle_write = operation_specs.WorkerInMemoryWrite(
        output_buffer=output_buffer,
        write_windowed_values=False,
        input=(producer_index, output_index),
        output_coders=[windowed_ungrouped_element_coder])
    self.map_tasks[map_task_index].append(
        (transform_node.full_label + '/Write', shuffle_write))

    output_map_task_index = self._run_read_from(
        transform_node, output_buffer.source())
    self.dependencies[output_map_task_index].add(map_task_index)

  def run_Flatten(self, transform_node):
    output_buffer = OutputBuffer(self._get_coder(transform_node.outputs[None]))
    output_map_task = self._run_read_from(transform_node,
                                          output_buffer.source())

    for input in transform_node.inputs:
      map_task_index, producer_index, output_index = self.outputs[input]
      element_coder = self._get_coder(input)
      flatten_write = operation_specs.WorkerInMemoryWrite(
          output_buffer=output_buffer,
          write_windowed_values=True,
          input=(producer_index, output_index),
          output_coders=[element_coder])
      self.map_tasks[map_task_index].append(
          (transform_node.full_label + '/Write', flatten_write))
      self.dependencies[output_map_task].add(map_task_index)

  def apply_CombinePerKey(self, transform, input):
    # TODO(robertwb): Support side inputs.
    assert not transform.args and not transform.kwargs
    return (input
            | PartialGroupByKeyCombineValues(transform.fn)
            | beam.GroupByKey()
            | MergeAccumulators(transform.fn)
            | ExtractOutputs(transform.fn))

  def run_PartialGroupByKeyCombineValues(self, transform_node):
    element_coder = self._get_coder(transform_node.outputs[None])
    _, producer_index, output_index = self.outputs[transform_node.inputs[0]]
    combine_op = operation_specs.WorkerPartialGroupByKey(
        combine_fn=pickler.dumps(
            (transform_node.transform.combine_fn, (), {}, ())),
        output_coders=[element_coder],
        input=(producer_index, output_index))
    self._run_as_op(transform_node, combine_op)

  def run_MergeAccumulators(self, transform_node):
    self._run_combine_transform(transform_node, 'merge')

  def run_ExtractOutputs(self, transform_node):
    self._run_combine_transform(transform_node, 'extract')

  def _run_combine_transform(self, transform_node, phase):
    transform = transform_node.transform
    element_coder = self._get_coder(transform_node.outputs[None])
    _, producer_index, output_index = self.outputs[transform_node.inputs[0]]
    combine_op = operation_specs.WorkerCombineFn(
        serialized_fn=pickler.dumps(
            (transform.combine_fn, (), {}, ())),
        phase=phase,
        output_coders=[element_coder],
        input=(producer_index, output_index))
    self._run_as_op(transform_node, combine_op)

  def _get_coder(self, pvalue, windowed=True):
    # TODO(robertwb): This should be an attribute of the pvalue itself.
    return DataflowRunner._get_coder(
        pvalue.element_type or typehints.Any,
        pvalue.windowing.windowfn.get_window_coder() if windowed else None)

  def _run_as_op(self, transform_node, op):
    """Single-output operation in the same map task as its input."""
    map_task_index, _, _ = self.outputs[transform_node.inputs[0]]
    op_index = len(self.map_tasks[map_task_index])
    output = transform_node.outputs[None]
    self.outputs[output] = map_task_index, op_index, 0
    self.map_tasks[map_task_index].append((transform_node.full_label, op))


class InMemorySource(iobase.BoundedSource):
  """Source for reading an (as-yet unwritten) set of in-memory encoded elements.
  """

  def __init__(self, encoded_elements, coder):
    self._encoded_elements = encoded_elements
    self._coder = coder

  def get_range_tracker(self, unused_start_position, unused_end_position):
    return None

  def read(self, unused_range_tracker):
    for encoded_element in self._encoded_elements:
      yield self._coder.decode(encoded_element)

  def default_output_coder(self):
    return self._coder


class OutputBuffer(object):

  def __init__(self, coder):
    self.coder = coder
    self.elements = []
    self.encoded_elements = []

  def source(self):
    return InMemorySource(self.encoded_elements, self.coder)

  def source_bundle(self):
    return iobase.SourceBundle(
        1.0, InMemorySource(self.encoded_elements, self.coder), None, None)

  def __repr__(self):
    return 'GroupingOutput[%r]' % len(self.elements)

  def append(self, value):
    self.elements.append(value)
    self.encoded_elements.append(self.coder.encode(value))


class GroupingOutputBuffer(object):

  def __init__(self, grouped_coder):
    self.grouped_coder = grouped_coder
    self.elements = collections.defaultdict(list)
    self.frozen = False

  def source(self):
    return InMemorySource(self.encoded_elements, self.grouped_coder)

  def __repr__(self):
    return 'GroupingOutputBuffer[%r]' % len(self.elements)

  def append(self, pair):
    assert not self.frozen
    k, v = pair
    self.elements[k].append(v)

  def freeze(self):
    if not self.frozen:
      self._encoded_elements = [self.grouped_coder.encode(kv)
                                for kv in self.elements.iteritems()]
    self.frozen = True
    return self._encoded_elements

  @property
  def encoded_elements(self):
    return GroupedOutputBuffer(self)


class GroupedOutputBuffer(object):

  def __init__(self, buffer):
    self.buffer = buffer

  def __getitem__(self, ix):
    return self.buffer.freeze()[ix]

  def __iter__(self):
    return iter(self.buffer.freeze())

  def __len__(self):
    return len(self.buffer.freeze())

  def __nonzero__(self):
    return True


class PartialGroupByKeyCombineValues(beam.PTransform):

  def __init__(self, combine_fn, native=True):
    self.combine_fn = combine_fn
    self.native = native

  def expand(self, input):
    if self.native:
      return beam.pvalue.PCollection(input.pipeline)
    else:
      def to_accumulator(v):
        return self.combine_fn.add_input(
            self.combine_fn.create_accumulator(), v)
      return input | beam.Map(lambda k_v: (k_v[0], to_accumulator(k_v[1])))


class MergeAccumulators(beam.PTransform):

  def __init__(self, combine_fn, native=True):
    self.combine_fn = combine_fn
    self.native = native

  def expand(self, input):
    if self.native:
      return beam.pvalue.PCollection(input.pipeline)
    else:
      merge_accumulators = self.combine_fn.merge_accumulators

      def merge_with_existing_key(k_vs):
        return (k_vs[0], merge_accumulators(k_vs[1]))

      return input | beam.Map(merge_with_existing_key)


class ExtractOutputs(beam.PTransform):

  def __init__(self, combine_fn, native=True):
    self.combine_fn = combine_fn
    self.native = native

  def expand(self, input):
    if self.native:
      return beam.pvalue.PCollection(input.pipeline)
    else:
      extract_output = self.combine_fn.extract_output
      return input | beam.Map(lambda k_v1: (k_v1[0], extract_output(k_v1[1])))


class WorkerRunnerResult(PipelineResult):

  def wait_until_finish(self, duration=None):
    pass
