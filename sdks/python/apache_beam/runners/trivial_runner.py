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

import collections
import logging
from typing import Any
from typing import Iterable
from typing import Iterator
from typing import List
from typing import TypeVar

from apache_beam import coders
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import common
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner import worker_handlers
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import core
from apache_beam.transforms import trigger
from apache_beam.utils import windowed_value

T = TypeVar("T")

_LOGGER = logging.getLogger(__name__)


class TrivialRunner(runner.PipelineRunner):
  """A bare-bones batch Python pipeline runner illistrating how to use the
  RunnerAPI and FnAPI to execute pipelines.

  Note that this runner is primarily for pedagogical purposes and is missing
  several features in order to keep it as simple as possible.  Where possible
  pointers are provided which this should serve as a useful starting point.
  """
  def run_portable_pipeline(self, pipeline, options):
    # First ensure we are able to run this pipeline.
    # Specifically, that it does not depend on requirements that were
    # added since this runner was developed.
    self.check_requirements(pipeline, self.supported_requirements())

    # Now we optimize the pipeline, notably performing pipeline fusion,
    # to turn it into a DAG where all the operations are one of
    # Impulse, Flatten, GroupByKey, or beam:runner:executable_stage.
    optimized_pipeline = translations.optimize_pipeline(
        pipeline,
        phases=translations.standard_optimize_phases(),
        known_runner_urns=frozenset([
            common_urns.primitives.IMPULSE.urn,
            common_urns.primitives.FLATTEN.urn,
            common_urns.primitives.GROUP_BY_KEY.urn
        ]),
        # This boolean indicates we want fused executable_stages.
        partial=False)

    # standard_optimize_phases() has a final step giving the stages in
    # topological order, so now we can just walk over them and execute
    # them.  (This are not quite so simple if we were attempting to execute
    # a streaming pipeline, but this is a trivial runner that only supports
    # batch...)
    execution_state = ExecutionState(optimized_pipeline)
    for transform_id in optimized_pipeline.root_transform_ids:
      self.execute_transform(transform_id, execution_state)

    # A more sophisticated runner may perform the execution in the background
    # and return a PipelineResult that can be used to monitor/cancel the
    # concurrent execution.
    return runner.PipelineResult(runner.PipelineState.DONE)

  def execute_transform(self, transform_id, execution_state):
    """Execute a single transform."""
    transform_proto = execution_state.optimized_pipeline.components.transforms[
        transform_id]
    _LOGGER.info(
        "Executing stage %s %s", transform_id, transform_proto.unique_name)
    if not is_primitive_transform(transform_proto):
      # A composite is simply executed by executing its parts.
      for sub_transform in transform_proto.subtransforms:
        self.execute_transform(sub_transform, execution_state)

    elif transform_proto.spec.urn == common_urns.primitives.IMPULSE.urn:
      # An impulse has no inputs and produces a single output (which happens
      # to be an empty byte string in the global window).
      execution_state.set_pcollection_contents(
          only_element(transform_proto.outputs.values()),
          [common.ENCODED_IMPULSE_VALUE])

    elif transform_proto.spec.urn == common_urns.primitives.FLATTEN.urn:
      # The output of a flatten is simply the union of its inputs.
      output_pcoll_id = only_element(transform_proto.outputs.values())
      execution_state.set_pcollection_contents(
          output_pcoll_id,
          sum([
              execution_state.get_pcollection_contents(pc)
              for pc in transform_proto.inputs.values()
          ], []))

    elif transform_proto.spec.urn == common_urns.executable_stage:
      # This is a collection of user DoFns.
      self.execute_executable_stage(transform_proto, execution_state)

    elif transform_proto.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
      # Execute the grouping operation.
      self.group_by_key_and_window(
          only_element(transform_proto.inputs.values()),
          only_element(transform_proto.outputs.values()),
          execution_state)

    else:
      raise RuntimeError(
          f"Unsupported transform {transform_id}"
          " of type {transform_proto.spec.urn}")

  def execute_executable_stage(self, transform_proto, execution_state):
    # Stage here is like a mini pipeline, with PTransforms, PCollections, etc.
    # inside of it.
    stage = beam_runner_api_pb2.ExecutableStagePayload.FromString(
        transform_proto.spec.payload)
    if stage.side_inputs:
      # To support these we would need to make the side input PCollections
      # available over the state API before processing this bundle.
      raise NotImplementedError()

    # This is the set of transforms that were fused together.
    stage_transforms = {
        id: stage.components.transforms[id]
        for id in stage.transforms
    }
    # The executable stage has bare PCollections as its inputs and outputs.
    #
    # We need an operation to feed data into the bundle (runner -> SDK).
    # This is done by attaching special transform that reads from the data
    # channel.
    input_transform = execution_state.new_id('stage_input')
    input_pcoll = stage.input
    stage_transforms[input_transform] = beam_runner_api_pb2.PTransform(
        # Read data, encoded with the given coder, from the data channel.
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=bundle_processor.DATA_INPUT_URN,
            payload=beam_fn_api_pb2.RemoteGrpcPort(
                # If we were using a cross-process data channel, we would also
                # need to set the address of the data channel itself here.
                coder_id=execution_state.windowed_coder_id(
                    stage.input)).SerializeToString()),
        # Wire its "output" to the required PCollection.
        outputs={'out': input_pcoll})
    # Also add operations to consume data produced by the bundle and writes
    # them to the data channel (SDK -> runner).
    output_ops_to_pcoll = {}
    for output_pcoll in stage.outputs:
      output_transform = execution_state.new_id('stage_output')
      stage_transforms[output_transform] = beam_runner_api_pb2.PTransform(
          # Data will be written here, with the given coder.
          spec=beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_OUTPUT_URN,
              payload=beam_fn_api_pb2.RemoteGrpcPort(
                  # Again, the grpc address itself is implicit.
                  coder_id=execution_state.windowed_coder_id(
                      output_pcoll)).SerializeToString()),
          # This operation takes as input the bundle's output pcollection.
          inputs={'input': output_pcoll})
      output_ops_to_pcoll[output_transform] = output_pcoll

    # Now we can create a description of what it means to process a bundle
    # of this type.  When processing the bundle, we simply refer to this
    # descriptor by id.  (In our case we only do so once, but in a more general
    # runner one may want to process the same "type" of bundle of many distinct
    # partitions of the input, especially in streaming where one may have
    # hundreds of concurrently processed bundles per key.)
    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=execution_state.new_id('descriptor'),
        transforms=stage_transforms,
        pcollections=stage.components.pcollections,
        coders=execution_state.optimized_pipeline.components.coders,
        windowing_strategies=stage.components.windowing_strategies,
        environments=stage.components.environments,
        # Were timers and state supported, their endpoints would be listed here.
    )
    execution_state.register_process_bundle_descriptor(
        process_bundle_descriptor)

    # Now we are ready to actually execute the bundle.
    process_bundle_id = execution_state.new_id('bundle')

    # First, push the all input data onto the data channel.
    # Timers would be sent over this channel as well.
    # In a real runner we could do this after (or concurrently with) starting
    # the bundle to avoid having to hold all the data to process in memory,
    # but for simplicity our bundle invocation (below) is synchronous so the
    # data must be available for processing right away.
    to_worker = execution_state.worker_handler.data_conn.output_stream(
        process_bundle_id, input_transform)
    for encoded_data in execution_state.get_pcollection_contents(input_pcoll):
      to_worker.write(encoded_data)
    to_worker.close()

    # Now we send a process bundle request over the control plane.
    process_bundle_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_id=process_bundle_descriptor.id))
    result_future = execution_state.worker_handler.control_conn.push(
        process_bundle_request)

    # Read the results off the data channel.
    # Note that if there are multiple outputs, we may get them in any order,
    # possibly interleaved.
    for output in execution_state.worker_handler.data_conn.input_elements(
        process_bundle_id, list(output_ops_to_pcoll.keys())):
      if isinstance(output, beam_fn_api_pb2.Elements.Data):
        # Adds the output to the appropriate PCollection.
        execution_state.set_pcollection_contents(
            output_ops_to_pcoll[output.transform_id], [output.data])
      else:
        # E.g. timers to set.
        raise RuntimeError("Unexpected data type: %s" % output)

    # Ensure the operation completed successfully.
    # This result contains things like metrics and continuation tokens as well.
    result = result_future.get()
    if result.error:
      raise RuntimeError(result.error)
    if result.process_bundle.residual_roots:
      # We would need to re-schedule execution of this bundle with this data.
      raise NotImplementedError('SDF continuation')
    if result.process_bundle.requires_finalization:
      # We would need to invoke the finalization callback, on a best effort
      # basis, *after* the outputs are durably committed.
      raise NotImplementedError('finalization')
    if result.process_bundle.elements.data:
      # These should be processed just like outputs from the data channel.
      raise NotImplementedError('control-channel data')
    if result.process_bundle.elements.timers:
      # These should be processed just like outputs from the data channel.
      raise NotImplementedError('timers')

  def group_by_key_and_window(self, input_pcoll, output_pcoll, execution_state):
    """Groups the elements of input_pcoll, placing their output in output_pcoll.
    """
    # Note that we are using the designated coders to decode and deeply inspect
    # the elements. This could be useful for other operations as well (e.g.
    # splitting the returned bundles of encoded elements into smaller chunks).
    # One issue that we're sweeping under the rug here is that for a truly
    # portable multi-language runner we may not be able to instanciate
    # every coder in Python. This can be overcome by wrapping unknown coders as
    # length-prefixing (or otherwise delimiting) variants.

    # Decode the input elements to get at their individual keys and values.
    input_coder = execution_state.windowed_coder(input_pcoll)
    key_coder = input_coder.key_coder()
    input_elements = []
    for encoded_elements in execution_state.get_pcollection_contents(
        input_pcoll):
      for element in decode_all(encoded_elements, input_coder):
        input_elements.append(element)

    # Now perform the actual grouping.
    components = execution_state.optimized_pipeline.components
    windowing = components.windowing_strategies[
        components.pcollections[input_pcoll].windowing_strategy_id]

    if (windowing.merge_status
        == beam_runner_api_pb2.MergeStatus.Enum.NON_MERGING and
        windowing.output_time
        == beam_runner_api_pb2.OutputTime.Enum.END_OF_WINDOW):
      # This is the "easy" case, show how to do it by hand.
      # Note that we're grouping by encoded key, and also by the window.
      grouped = collections.defaultdict(list)
      for element in input_elements:
        for window in element.windows:
          key, value = element.value
          grouped[window, key_coder.encode(key)].append(value)
      output_elements = [
          windowed_value.WindowedValue(
              (key_coder.decode(encoded_key), values),
              window.end, [window],
              trigger.BatchGlobalTriggerDriver.ONLY_FIRING)
          for ((window, encoded_key), values) in grouped.items()
      ]
    else:
      # This handles generic merging and triggering.
      trigger_driver = trigger.create_trigger_driver(
          execution_state.windowing_strategy(input_pcoll), True)
      grouped_by_key = collections.defaultdict(list)
      for element in input_elements:
        key, value = element.value
        grouped_by_key[key_coder.encode(key)].append(element.with_value(value))
      output_elements = []
      for encoded_key, windowed_values in grouped_by_key.items():
        for grouping in trigger_driver.process_entire_key(
            key_coder.decode(encoded_key), windowed_values):
          output_elements.append(grouping)

    # Store the grouped values in the output PCollection.
    output_coder = execution_state.windowed_coder(output_pcoll)
    execution_state.set_pcollection_contents(
        output_pcoll, [encode_all(output_elements, output_coder)])

  def supported_requirements(self) -> Iterable[str]:
    # Nothing non-trivial is supported.
    return []


class ExecutionState:
  """A helper class holding various values and context during execution."""
  def __init__(self, optimized_pipeline):
    self.optimized_pipeline = optimized_pipeline
    self._pcollections_to_encoded_chunks = {}
    self._counter = 0
    self._process_bundle_descriptors = {}
    # This emulates a connection to an SDK worker (e.g. its data, control,
    # etc. channels).
    # There are other variants available as well (e.g. GRPC, Docker, ...)
    self.worker_handler = worker_handlers.EmbeddedWorkerHandler(
        None,
        state=worker_handlers.StateServicer(),
        provision_info=None,
        worker_manager=self)
    self._windowed_coders = {}
    # Populate all windowed coders before creating _pipeline_context.
    for pcoll_id in self.optimized_pipeline.components.pcollections.keys():
      self.windowed_coder_id(pcoll_id)
    self._pipeline_context = pipeline_context.PipelineContext(
        optimized_pipeline.components)

  def register_process_bundle_descriptor(
      self, process_bundle_descriptor: beam_fn_api_pb2.ProcessBundleDescriptor):
    self._process_bundle_descriptors[
        process_bundle_descriptor.id] = process_bundle_descriptor

  def get_pcollection_contents(self, pcoll_id: str) -> List[bytes]:
    return self._pcollections_to_encoded_chunks[pcoll_id]

  def set_pcollection_contents(self, pcoll_id: str, chunks: List[bytes]):
    self._pcollections_to_encoded_chunks[pcoll_id] = chunks

  def new_id(self, prefix='') -> str:
    self._counter += 1
    return f'runner_{prefix}_{self._counter}'

  def windowed_coder(self, pcollection_id: str) -> coders.Coder:
    return self._pipeline_context.coders.get_by_id(
        self.windowed_coder_id(pcollection_id))

  def windowing_strategy(self, pcollection_id: str) -> core.Windowing:
    return self._pipeline_context.windowing_strategies.get_by_id(
        self.optimized_pipeline.components.pcollections[pcollection_id].
        windowing_strategy_id)

  def windowed_coder_id(self, pcollection_id: str) -> str:
    pcoll = self.optimized_pipeline.components.pcollections[pcollection_id]
    windowing = self.optimized_pipeline.components.windowing_strategies[
        pcoll.windowing_strategy_id]
    return self._windowed_coder_id_from(
        pcoll.coder_id, windowing.window_coder_id)

  def _windowed_coder_id_from(self, coder_id: str, window_coder_id: str) -> str:
    if (coder_id, window_coder_id) not in self._windowed_coders:
      windowed_coder_id = self.new_id('windowed_coder')
      self._windowed_coders[coder_id, window_coder_id] = windowed_coder_id
      self.optimized_pipeline.components.coders[windowed_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.WINDOWED_VALUE.urn),
              component_coder_ids=[coder_id, window_coder_id]))
    return self._windowed_coders[coder_id, window_coder_id]


def is_primitive_transform(transform: beam_runner_api_pb2.PTransform) -> bool:
  # pylint: disable=consider-using-ternary
  return (
      # Only non-primitive (aka composite) transforms have subtransforms.
      not transform.subtransforms
      # If it has outputs but all of the outputs are also inputs to this
      # transform, there is nothing to compute.
      and not transform.outputs or
      bool(set(transform.outputs.values()) - set(transform.inputs.values())))


def only_element(iterable: Iterable[T]) -> T:
  element, = iterable
  return element


def decode_all(encoded_elements: bytes, coder: coders.Coder) -> Iterator[Any]:
  coder_impl = coder.get_impl()
  input_stream = create_InputStream(encoded_elements)
  while input_stream.size() > 0:
    yield coder_impl.decode_from_stream(input_stream, True)


def encode_all(elements: Iterator[T], coder: coders.Coder) -> bytes:
  coder_impl = coder.get_impl()
  output_stream = create_OutputStream()
  for element in elements:
    coder_impl.encode_to_stream(element, output_stream, True)
  return output_stream.get()
