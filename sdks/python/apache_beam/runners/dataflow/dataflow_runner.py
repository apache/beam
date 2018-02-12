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

"""A runner implementation that submits a job for remote execution.

The runner will create a JSON description of the job graph and then submit it
to the Dataflow Service for remote execution by a worker.
"""

import logging
import threading
import time
import traceback
import urllib
from collections import defaultdict

import apache_beam as beam
from apache_beam import coders
from apache_beam import error
from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.internal.gcp import json_value
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.portability import common_urns
from apache_beam.pvalue import AsSideInput
from apache_beam.runners.dataflow.dataflow_metrics import DataflowMetrics
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.dataflow.internal.names import TransformNames
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PValueCache
from apache_beam.transforms.display import DisplayData
from apache_beam.typehints import typehints
from apache_beam.utils.plugin import BeamPlugin

__all__ = ['DataflowRunner']


class DataflowRunner(PipelineRunner):
  """A runner that creates job graphs and submits them for remote execution.

  Every execution of the run() method will submit an independent job for
  remote execution that consists of the nodes reachable from the passed in
  node argument or entire graph if node is None. The run() method returns
  after the service created the job and  will not wait for the job to finish
  if blocking is set to False.
  """

  # A list of PTransformOverride objects to be applied before running a pipeline
  # using DataflowRunner.
  # Currently this only works for overrides where the input and output types do
  # not change.
  # For internal SDK use only. This should not be updated by Beam pipeline
  # authors.

  # Imported here to avoid circular dependencies.
  # TODO: Remove the apache_beam.pipeline dependency in CreatePTransformOverride
  from apache_beam.runners.dataflow.ptransform_overrides import CreatePTransformOverride

  _PTRANSFORM_OVERRIDES = [
      CreatePTransformOverride(),
  ]

  def __init__(self, cache=None):
    # Cache of CloudWorkflowStep protos generated while the runner
    # "executes" a pipeline.
    self._cache = cache if cache is not None else PValueCache()
    self._unique_step_id = 0

  def _get_unique_step_name(self):
    self._unique_step_id += 1
    return 's%s' % self._unique_step_id

  @staticmethod
  def poll_for_job_completion(runner, result, duration):
    """Polls for the specified job to finish running (successfully or not).

    Updates the result with the new job information before returning.

    Args:
      runner: DataflowRunner instance to use for polling job state.
      result: DataflowPipelineResult instance used for job information.
      duration (int): The time to wait (in milliseconds) for job to finish.
        If it is set to :data:`None`, it will wait indefinitely until the job
        is finished.
    """
    last_message_time = None
    last_message_hash = None

    last_error_rank = float('-inf')
    last_error_msg = None
    last_job_state = None
    # How long to wait after pipeline failure for the error
    # message to show up giving the reason for the failure.
    # It typically takes about 30 seconds.
    final_countdown_timer_secs = 50.0
    sleep_secs = 5.0

    # Try to prioritize the user-level traceback, if any.
    def rank_error(msg):
      if 'work item was attempted' in msg:
        return -1
      elif 'Traceback' in msg:
        return 1
      return 0

    if duration:
      start_secs = time.time()
      duration_secs = duration / 1000

    job_id = result.job_id()
    while True:
      response = runner.dataflow_client.get_job(job_id)
      # If get() is called very soon after Create() the response may not contain
      # an initialized 'currentState' field.
      if response.currentState is not None:
        if response.currentState != last_job_state:
          logging.info('Job %s is in state %s', job_id, response.currentState)
          last_job_state = response.currentState
        if str(response.currentState) != 'JOB_STATE_RUNNING':
          # Stop checking for new messages on timeout, explanatory
          # message received, success, or a terminal job state caused
          # by the user that therefore doesn't require explanation.
          if (final_countdown_timer_secs <= 0.0
              or last_error_msg is not None
              or str(response.currentState) == 'JOB_STATE_DONE'
              or str(response.currentState) == 'JOB_STATE_CANCELLED'
              or str(response.currentState) == 'JOB_STATE_UPDATED'
              or str(response.currentState) == 'JOB_STATE_DRAINED'):
            break
          # The job has failed; ensure we see any final error messages.
          sleep_secs = 1.0      # poll faster during the final countdown
          final_countdown_timer_secs -= sleep_secs
      time.sleep(sleep_secs)

      # Get all messages since beginning of the job run or since last message.
      page_token = None
      while True:
        messages, page_token = runner.dataflow_client.list_messages(
            job_id, page_token=page_token, start_time=last_message_time)
        for m in messages:
          message = '%s: %s: %s' % (m.time, m.messageImportance, m.messageText)
          m_hash = hash(message)

          if last_message_hash is not None and m_hash == last_message_hash:
            # Skip the first message if it is the last message we got in the
            # previous round. This can happen because we use the
            # last_message_time as a parameter of the query for new messages.
            continue
          last_message_time = m.time
          last_message_hash = m_hash
          # Skip empty messages.
          if m.messageImportance is None:
            continue
          logging.info(message)
          if str(m.messageImportance) == 'JOB_MESSAGE_ERROR':
            if rank_error(m.messageText) >= last_error_rank:
              last_error_rank = rank_error(m.messageText)
              last_error_msg = m.messageText
        if not page_token:
          break

      if duration:
        passed_secs = time.time() - start_secs
        if passed_secs > duration_secs:
          logging.warning('Timing out on waiting for job %s after %d seconds',
                          job_id, passed_secs)
          break

    result._job = response
    runner.last_error_msg = last_error_msg

  @staticmethod
  def group_by_key_input_visitor():
    # Imported here to avoid circular dependencies.
    from apache_beam.pipeline import PipelineVisitor

    class GroupByKeyInputVisitor(PipelineVisitor):
      """A visitor that replaces `Any` element type for input `PCollection` of
      a `GroupByKey` or `_GroupByKeyOnly` with a `KV` type.

      TODO(BEAM-115): Once Python SDk is compatible with the new Runner API,
      we could directly replace the coder instead of mutating the element type.
      """

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        # Imported here to avoid circular dependencies.
        # pylint: disable=wrong-import-order, wrong-import-position
        from apache_beam.transforms.core import GroupByKey, _GroupByKeyOnly
        if isinstance(transform_node.transform, (GroupByKey, _GroupByKeyOnly)):
          pcoll = transform_node.inputs[0]
          input_type = pcoll.element_type
          # If input_type is not specified, then treat it as `Any`.
          if not input_type:
            input_type = typehints.Any

          def coerce_to_kv_type(element_type):
            if isinstance(element_type, typehints.TupleHint.TupleConstraint):
              if len(element_type.tuple_types) == 2:
                return element_type
              else:
                raise ValueError(
                    "Tuple input to GroupByKey must be have two components. "
                    "Found %s for %s" % (element_type, pcoll))
            elif isinstance(input_type, typehints.AnyTypeConstraint):
              # `Any` type needs to be replaced with a KV[Any, Any] to
              # force a KV coder as the main output coder for the pcollection
              # preceding a GroupByKey.
              return typehints.KV[typehints.Any, typehints.Any]
            elif isinstance(element_type, typehints.UnionConstraint):
              union_types = [
                  coerce_to_kv_type(t) for t in element_type.union_types]
              return typehints.KV[
                  typehints.Union[tuple(t.tuple_types[0] for t in union_types)],
                  typehints.Union[tuple(t.tuple_types[1] for t in union_types)]]
            else:
              # TODO: Possibly handle other valid types.
              raise ValueError(
                  "Input to GroupByKey must be of Tuple or Any type. "
                  "Found %s for %s" % (element_type, pcoll))
          pcoll.element_type = coerce_to_kv_type(input_type)
          key_type, value_type = pcoll.element_type.tuple_types
          if transform_node.outputs:
            transform_node.outputs[None].element_type = typehints.KV[
                key_type, typehints.Iterable[value_type]]

    return GroupByKeyInputVisitor()

  @staticmethod
  def flatten_input_visitor():
    # Imported here to avoid circular dependencies.
    from apache_beam.pipeline import PipelineVisitor

    class FlattenInputVisitor(PipelineVisitor):
      """A visitor that replaces the element type for input ``PCollections``s of
       a ``Flatten`` transform with that of the output ``PCollection``.
      """

      def visit_transform(self, transform_node):
        # Imported here to avoid circular dependencies.
        # pylint: disable=wrong-import-order, wrong-import-position
        from apache_beam import Flatten
        if isinstance(transform_node.transform, Flatten):
          output_pcoll = transform_node.outputs[None]
          for input_pcoll in transform_node.inputs:
            input_pcoll.element_type = output_pcoll.element_type

    return FlattenInputVisitor()

  def run_pipeline(self, pipeline):
    """Remotely executes entire pipeline or parts reachable from node."""
    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.runners.dataflow.internal import apiclient
    except ImportError:
      raise ImportError(
          'Google Cloud Dataflow runner not available, '
          'please install apache_beam[gcp]')

    # Snapshot the pipeline in a portable proto before mutating it
    proto_pipeline, self.proto_context = pipeline.to_runner_api(
        return_context=True)

    # Performing configured PTransform overrides.
    pipeline.replace_all(DataflowRunner._PTRANSFORM_OVERRIDES)

    # Add setup_options for all the BeamPlugin imports
    setup_options = pipeline._options.view_as(SetupOptions)
    plugins = BeamPlugin.get_all_plugin_paths()
    if setup_options.beam_plugins is not None:
      plugins = list(set(plugins + setup_options.beam_plugins))
    setup_options.beam_plugins = plugins

    self.job = apiclient.Job(pipeline._options, proto_pipeline)

    # Dataflow runner requires a KV type for GBK inputs, hence we enforce that
    # here.
    pipeline.visit(self.group_by_key_input_visitor())

    # Dataflow runner requires output type of the Flatten to be the same as the
    # inputs, hence we enforce that here.
    pipeline.visit(self.flatten_input_visitor())

    # The superclass's run will trigger a traversal of all reachable nodes.
    super(DataflowRunner, self).run_pipeline(pipeline)

    test_options = pipeline._options.view_as(TestOptions)
    # If it is a dry run, return without submitting the job.
    if test_options.dry_run:
      return None

    # Get a Dataflow API client and set its options
    self.dataflow_client = apiclient.DataflowApplicationClient(
        pipeline._options)

    # Create the job description and send a request to the service. The result
    # can be None if there is no need to send a request to the service (e.g.
    # template creation). If a request was sent and failed then the call will
    # raise an exception.
    result = DataflowPipelineResult(
        self.dataflow_client.create_job(self.job), self)

    self._metrics = DataflowMetrics(self.dataflow_client, result, self.job)
    result.metric_results = self._metrics
    return result

  def _get_typehint_based_encoding(self, typehint, window_coder):
    """Returns an encoding based on a typehint object."""
    return self._get_cloud_encoding(self._get_coder(typehint,
                                                    window_coder=window_coder))

  @staticmethod
  def _get_coder(typehint, window_coder):
    """Returns a coder based on a typehint object."""
    if window_coder:
      return coders.WindowedValueCoder(
          coders.registry.get_coder(typehint),
          window_coder=window_coder)
    return coders.registry.get_coder(typehint)

  def _get_cloud_encoding(self, coder):
    """Returns an encoding based on a coder object."""
    if not isinstance(coder, coders.Coder):
      raise TypeError('Coder object must inherit from coders.Coder: %s.' %
                      str(coder))
    return coder.as_cloud_object()

  def _get_side_input_encoding(self, input_encoding):
    """Returns an encoding for the output of a view transform.

    Args:
      input_encoding: encoding of current transform's input. Side inputs need
        this because the service will check that input and output types match.

    Returns:
      An encoding that matches the output and input encoding. This is essential
      for the View transforms introduced to produce side inputs to a ParDo.
    """
    return {
        '@type': input_encoding['@type'],
        'component_encodings': [input_encoding]
    }

  def _get_encoded_output_coder(self, transform_node, window_value=True):
    """Returns the cloud encoding of the coder for the output of a transform."""
    if (len(transform_node.outputs) == 1
        and transform_node.outputs[None].element_type is not None):
      # TODO(robertwb): Handle type hints for multi-output transforms.
      element_type = transform_node.outputs[None].element_type
    else:
      # TODO(silviuc): Remove this branch (and assert) when typehints are
      # propagated everywhere. Returning an 'Any' as type hint will trigger
      # usage of the fallback coder (i.e., cPickler).
      element_type = typehints.Any
    if window_value:
      window_coder = (
          transform_node.outputs[None].windowing.windowfn.get_window_coder())
    else:
      window_coder = None
    return self._get_typehint_based_encoding(
        element_type, window_coder=window_coder)

  def _add_step(self, step_kind, step_label, transform_node, side_tags=()):
    """Creates a Step object and adds it to the cache."""
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.internal import apiclient
    step = apiclient.Step(step_kind, self._get_unique_step_name())
    self.job.proto.steps.append(step.proto)
    step.add_property(PropertyNames.USER_NAME, step_label)
    # Cache the node/step association for the main output of the transform node.
    self._cache.cache_output(transform_node, None, step)
    # If side_tags is not () then this is a multi-output transform node and we
    # need to cache the (node, tag, step) for each of the tags used to access
    # the outputs. This is essential because the keys used to search in the
    # cache always contain the tag.
    for tag in side_tags:
      self._cache.cache_output(transform_node, tag, step)

    # Finally, we add the display data items to the pipeline step.
    # If the transform contains no display data then an empty list is added.
    step.add_property(
        PropertyNames.DISPLAY_DATA,
        [item.get_dict() for item in
         DisplayData.create_from(transform_node.transform).items])

    return step

  def _add_singleton_step(self, label, full_label, tag, input_step):
    """Creates a CollectionToSingleton step used to handle ParDo side inputs."""
    # Import here to avoid adding the dependency for local running scenarios.
    from apache_beam.runners.dataflow.internal import apiclient
    step = apiclient.Step(TransformNames.COLLECTION_TO_SINGLETON, label)
    self.job.proto.steps.append(step.proto)
    step.add_property(PropertyNames.USER_NAME, full_label)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(tag)})
    step.encoding = self._get_side_input_encoding(input_step.encoding)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (full_label, PropertyNames.OUTPUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])
    return step

  def run_Impulse(self, transform_node):
    standard_options = (
        transform_node.outputs[None].pipeline._options.view_as(StandardOptions))
    if standard_options.streaming:
      step = self._add_step(
          TransformNames.READ, transform_node.full_label, transform_node)
      step.add_property(PropertyNames.FORMAT, 'pubsub')
      step.add_property(PropertyNames.PUBSUB_SUBSCRIPTION, '_starting_signal/')

      step.encoding = self._get_encoded_output_coder(transform_node)
      step.add_property(
          PropertyNames.OUTPUT_INFO,
          [{PropertyNames.USER_NAME: (
              '%s.%s' % (
                  transform_node.full_label, PropertyNames.OUT)),
            PropertyNames.ENCODING: step.encoding,
            PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])
    else:
      ValueError('Impulse source for batch pipelines has not been defined.')

  def run_Flatten(self, transform_node):
    step = self._add_step(TransformNames.FLATTEN,
                          transform_node.full_label, transform_node)
    inputs = []
    for one_input in transform_node.inputs:
      input_step = self._cache.get_pvalue(one_input)
      inputs.append(
          {'@type': 'OutputReference',
           PropertyNames.STEP_NAME: input_step.proto.name,
           PropertyNames.OUTPUT_NAME: input_step.get_output(one_input.tag)})
    step.add_property(PropertyNames.INPUTS, inputs)
    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])

  def apply_WriteToBigQuery(self, transform, pcoll):
    # Make sure this is the WriteToBigQuery class that we expected
    if not isinstance(transform, beam.io.WriteToBigQuery):
      return self.apply_PTransform(transform, pcoll)
    standard_options = pcoll.pipeline._options.view_as(StandardOptions)
    if standard_options.streaming:
      if (transform.write_disposition ==
          beam.io.BigQueryDisposition.WRITE_TRUNCATE):
        raise RuntimeError('Can not use write truncation mode in streaming')
      return self.apply_PTransform(transform, pcoll)
    else:
      return pcoll  | 'WriteToBigQuery' >> beam.io.Write(
          beam.io.BigQuerySink(
              transform.table_reference.tableId,
              transform.table_reference.datasetId,
              transform.table_reference.projectId,
              transform.schema,
              transform.create_disposition,
              transform.write_disposition))

  def apply_GroupByKey(self, transform, pcoll):
    # Infer coder of parent.
    #
    # TODO(ccy): make Coder inference and checking less specialized and more
    # comprehensive.
    parent = pcoll.producer
    if parent:
      coder = parent.transform._infer_output_coder()  # pylint: disable=protected-access
    if not coder:
      coder = self._get_coder(pcoll.element_type or typehints.Any, None)
    if not coder.is_kv_coder():
      raise ValueError(('Coder for the GroupByKey operation "%s" is not a '
                        'key-value coder: %s.') % (transform.label,
                                                   coder))
    # TODO(robertwb): Update the coder itself if it changed.
    coders.registry.verify_deterministic(
        coder.key_coder(), 'GroupByKey operation "%s"' % transform.label)

    return pvalue.PCollection(pcoll.pipeline)

  def run_GroupByKey(self, transform_node):
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step = self._add_step(
        TransformNames.GROUP, transform_node.full_label, transform_node)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    step.encoding = self._get_encoded_output_coder(transform_node)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])
    windowing = transform_node.transform.get_windowing(
        transform_node.inputs)
    step.add_property(
        PropertyNames.SERIALIZED_FN,
        self.serialize_windowing_strategy(windowing))

  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])

    # Attach side inputs.
    si_dict = {}
    # We must call self._cache.get_pvalue exactly once due to refcounting.
    si_labels = {}
    full_label_counts = defaultdict(int)
    lookup_label = lambda side_pval: si_labels[side_pval]
    for side_pval in transform_node.side_inputs:
      assert isinstance(side_pval, AsSideInput)
      step_number = self._get_unique_step_name()
      si_label = 'SideInput-' + step_number
      pcollection_label = '%s.%s' % (
          side_pval.pvalue.producer.full_label.split('/')[-1],
          side_pval.pvalue.tag if side_pval.pvalue.tag else 'out')
      si_full_label = '%s/%s(%s.%s)' % (transform_node.full_label,
                                        side_pval.__class__.__name__,
                                        pcollection_label,
                                        full_label_counts[pcollection_label])

      # Count the number of times the same PCollection is a side input
      # to the same ParDo.
      full_label_counts[pcollection_label] += 1

      self._add_singleton_step(
          si_label, si_full_label, side_pval.pvalue.tag,
          self._cache.get_pvalue(side_pval.pvalue))
      si_dict[si_label] = {
          '@type': 'OutputReference',
          PropertyNames.STEP_NAME: si_label,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}
      si_labels[side_pval] = si_label

    # Now create the step for the ParDo transform being handled.
    transform_name = transform_node.full_label.rsplit('/', 1)[-1]
    step = self._add_step(
        TransformNames.DO,
        transform_node.full_label + (
            '/{}'.format(transform_name)
            if transform_node.side_inputs else ''),
        transform_node,
        transform_node.transform.output_tags)
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.internal import apiclient
    transform_proto = self.proto_context.transforms.get_proto(transform_node)
    if (apiclient._use_fnapi(transform_node.inputs[0].pipeline._options)
        and transform_proto.spec.urn == common_urns.PARDO_TRANSFORM):
      serialized_data = self.proto_context.transforms.get_id(transform_node)
    else:
      serialized_data = pickler.dumps(
          self._pardo_fn_data(transform_node, lookup_label))
    step.add_property(PropertyNames.SERIALIZED_FN, serialized_data)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    # Add side inputs if any.
    step.add_property(PropertyNames.NON_PARALLEL_INPUTS, si_dict)

    # Generate description for the outputs. The output names
    # will be 'out' for main output and 'out_<tag>' for a tagged output.
    # Using 'out' as a tag will not clash with the name for main since it will
    # be transformed into 'out_out' internally.
    outputs = []
    step.encoding = self._get_encoded_output_coder(transform_node)

    # Add the main output to the description.
    outputs.append(
        {PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
         PropertyNames.ENCODING: step.encoding,
         PropertyNames.OUTPUT_NAME: PropertyNames.OUT})
    for side_tag in transform.output_tags:
      # The assumption here is that all outputs will have the same typehint
      # and coder as the main output. This is certainly the case right now
      # but conceivably it could change in the future.
      outputs.append(
          {PropertyNames.USER_NAME: (
              '%s.%s' % (transform_node.full_label, side_tag)),
           PropertyNames.ENCODING: step.encoding,
           PropertyNames.OUTPUT_NAME: (
               '%s_%s' % (PropertyNames.OUT, side_tag))})

    step.add_property(PropertyNames.OUTPUT_INFO, outputs)

  @staticmethod
  def _pardo_fn_data(transform_node, get_label):
    transform = transform_node.transform
    si_tags_and_types = [  # pylint: disable=protected-access
        (get_label(side_pval), side_pval.__class__, side_pval._view_options())
        for side_pval in transform_node.side_inputs]
    return (transform.fn, transform.args, transform.kwargs, si_tags_and_types,
            transform_node.inputs[0].windowing)

  def apply_CombineValues(self, transform, pcoll):
    # TODO(BEAM-2937): Disable combiner lifting for fnapi. Remove this
    # restrictions once this feature is supported in the dataflow runner
    # harness.
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.internal import apiclient
    if apiclient._use_fnapi(pcoll.pipeline._options):
      return self.apply_PTransform(transform, pcoll)

    return pvalue.PCollection(pcoll.pipeline)

  def run_CombineValues(self, transform_node):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step = self._add_step(
        TransformNames.COMBINE, transform_node.full_label, transform_node)
    # Combiner functions do not take deferred side-inputs (i.e. PValues) and
    # therefore the code to handle extra args/kwargs is simpler than for the
    # DoFn's of the ParDo transform. In the last, empty argument is where
    # side inputs information would go.
    fn_data = (transform.fn, transform.args, transform.kwargs, ())
    step.add_property(PropertyNames.SERIALIZED_FN,
                      pickler.dumps(fn_data))
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    # Note that the accumulator must not have a WindowedValue encoding, while
    # the output of this step does in fact have a WindowedValue encoding.
    accumulator_encoding = self._get_cloud_encoding(
        transform_node.transform.fn.get_accumulator_coder())
    output_encoding = self._get_encoded_output_coder(transform_node)

    step.encoding = output_encoding
    step.add_property(PropertyNames.ENCODING, accumulator_encoding)
    # Generate description for main output 'out.'
    outputs = []
    # Add the main output to the description.
    outputs.append(
        {PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
         PropertyNames.ENCODING: step.encoding,
         PropertyNames.OUTPUT_NAME: PropertyNames.OUT})
    step.add_property(PropertyNames.OUTPUT_INFO, outputs)

  def run_Read(self, transform_node):
    transform = transform_node.transform
    step = self._add_step(
        TransformNames.READ, transform_node.full_label, transform_node)
    # TODO(mairbek): refactor if-else tree to use registerable functions.
    # Initialize the source specific properties.

    if not hasattr(transform.source, 'format'):
      # If a format is not set, we assume the source to be a custom source.
      source_dict = {}

      source_dict['spec'] = {
          '@type': names.SOURCE_TYPE,
          names.SERIALIZED_SOURCE_KEY: pickler.dumps(transform.source)
      }

      try:
        source_dict['metadata'] = {
            'estimated_size_bytes': json_value.get_typed_value_descriptor(
                transform.source.estimate_size())
        }
      except error.RuntimeValueProviderError:
        # Size estimation is best effort, and this error is by value provider.
        logging.info(
            'Could not estimate size of source %r due to ' + \
            'RuntimeValueProviderError', transform.source)
      except Exception:  # pylint: disable=broad-except
        # Size estimation is best effort. So we log the error and continue.
        logging.info(
            'Could not estimate size of source %r due to an exception: %s',
            transform.source, traceback.format_exc())

      step.add_property(PropertyNames.SOURCE_STEP_INPUT,
                        source_dict)
    elif transform.source.format == 'text':
      step.add_property(PropertyNames.FILE_PATTERN, transform.source.path)
    elif transform.source.format == 'bigquery':
      step.add_property(PropertyNames.BIGQUERY_EXPORT_FORMAT, 'FORMAT_AVRO')
      # TODO(silviuc): Add table validation if transform.source.validate.
      if transform.source.table_reference is not None:
        step.add_property(PropertyNames.BIGQUERY_DATASET,
                          transform.source.table_reference.datasetId)
        step.add_property(PropertyNames.BIGQUERY_TABLE,
                          transform.source.table_reference.tableId)
        # If project owning the table was not specified then the project owning
        # the workflow (current project) will be used.
        if transform.source.table_reference.projectId is not None:
          step.add_property(PropertyNames.BIGQUERY_PROJECT,
                            transform.source.table_reference.projectId)
      elif transform.source.query is not None:
        step.add_property(PropertyNames.BIGQUERY_QUERY, transform.source.query)
        step.add_property(PropertyNames.BIGQUERY_USE_LEGACY_SQL,
                          transform.source.use_legacy_sql)
        step.add_property(PropertyNames.BIGQUERY_FLATTEN_RESULTS,
                          transform.source.flatten_results)
      else:
        raise ValueError('BigQuery source %r must specify either a table or'
                         ' a query',
                         transform.source)
    elif transform.source.format == 'pubsub':
      standard_options = (
          transform_node.inputs[0].pipeline.options.view_as(StandardOptions))
      if not standard_options.streaming:
        raise ValueError('PubSubPayloadSource is currently available for use '
                         'only in streaming pipelines.')
      # Only one of topic or subscription should be set.
      if transform.source.full_subscription:
        step.add_property(PropertyNames.PUBSUB_SUBSCRIPTION,
                          transform.source.full_subscription)
      elif transform.source.full_topic:
        step.add_property(PropertyNames.PUBSUB_TOPIC,
                          transform.source.full_topic)
      if transform.source.id_label:
        step.add_property(PropertyNames.PUBSUB_ID_LABEL,
                          transform.source.id_label)
    else:
      raise ValueError(
          'Source %r has unexpected format %s.' % (
              transform.source, transform.source.format))

    if not hasattr(transform.source, 'format'):
      step.add_property(PropertyNames.FORMAT, names.SOURCE_FORMAT)
    else:
      step.add_property(PropertyNames.FORMAT, transform.source.format)

    # Wrap coder in WindowedValueCoder: this is necessary as the encoding of a
    # step should be the type of value outputted by each step.  Read steps
    # automatically wrap output values in a WindowedValue wrapper, if necessary.
    # This is also necessary for proper encoding for size estimation.
    # Using a GlobalWindowCoder as a place holder instead of the default
    # PickleCoder because GlobalWindowCoder is known coder.
    # TODO(robertwb): Query the collection for the windowfn to extract the
    # correct coder.
    coder = coders.WindowedValueCoder(transform._infer_output_coder(),
                                      coders.coders.GlobalWindowCoder())  # pylint: disable=protected-access

    step.encoding = self._get_cloud_encoding(coder)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])

  def run__NativeWrite(self, transform_node):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step = self._add_step(
        TransformNames.WRITE, transform_node.full_label, transform_node)
    # TODO(mairbek): refactor if-else tree to use registerable functions.
    # Initialize the sink specific properties.
    if transform.sink.format == 'text':
      # Note that it is important to use typed properties (@type/value dicts)
      # for non-string properties and also for empty strings. For example,
      # in the code below the num_shards must have type and also
      # file_name_suffix and shard_name_template (could be empty strings).
      step.add_property(
          PropertyNames.FILE_NAME_PREFIX, transform.sink.file_name_prefix,
          with_type=True)
      step.add_property(
          PropertyNames.FILE_NAME_SUFFIX, transform.sink.file_name_suffix,
          with_type=True)
      step.add_property(
          PropertyNames.SHARD_NAME_TEMPLATE, transform.sink.shard_name_template,
          with_type=True)
      if transform.sink.num_shards > 0:
        step.add_property(
            PropertyNames.NUM_SHARDS, transform.sink.num_shards, with_type=True)
      # TODO(silviuc): Implement sink validation.
      step.add_property(PropertyNames.VALIDATE_SINK, False, with_type=True)
    elif transform.sink.format == 'bigquery':
      # TODO(silviuc): Add table validation if transform.sink.validate.
      step.add_property(PropertyNames.BIGQUERY_DATASET,
                        transform.sink.table_reference.datasetId)
      step.add_property(PropertyNames.BIGQUERY_TABLE,
                        transform.sink.table_reference.tableId)
      # If project owning the table was not specified then the project owning
      # the workflow (current project) will be used.
      if transform.sink.table_reference.projectId is not None:
        step.add_property(PropertyNames.BIGQUERY_PROJECT,
                          transform.sink.table_reference.projectId)
      step.add_property(PropertyNames.BIGQUERY_CREATE_DISPOSITION,
                        transform.sink.create_disposition)
      step.add_property(PropertyNames.BIGQUERY_WRITE_DISPOSITION,
                        transform.sink.write_disposition)
      if transform.sink.table_schema is not None:
        step.add_property(
            PropertyNames.BIGQUERY_SCHEMA, transform.sink.schema_as_json())
    elif transform.sink.format == 'pubsub':
      standard_options = (
          transform_node.inputs[0].pipeline.options.view_as(StandardOptions))
      if not standard_options.streaming:
        raise ValueError('PubSubPayloadSink is currently available for use '
                         'only in streaming pipelines.')
      step.add_property(PropertyNames.PUBSUB_TOPIC, transform.sink.full_topic)
    else:
      raise ValueError(
          'Sink %r has unexpected format %s.' % (
              transform.sink, transform.sink.format))
    step.add_property(PropertyNames.FORMAT, transform.sink.format)

    # Wrap coder in WindowedValueCoder: this is necessary for proper encoding
    # for size estimation. Using a GlobalWindowCoder as a place holder instead
    # of the default PickleCoder because GlobalWindowCoder is known coder.
    # TODO(robertwb): Query the collection for the windowfn to extract the
    # correct coder.
    coder = coders.WindowedValueCoder(transform.sink.coder,
                                      coders.coders.GlobalWindowCoder())
    step.encoding = self._get_cloud_encoding(coder)
    step.add_property(PropertyNames.ENCODING, step.encoding)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})

  @classmethod
  def serialize_windowing_strategy(cls, windowing):
    from apache_beam.runners import pipeline_context
    from apache_beam.portability.api import beam_runner_api_pb2
    context = pipeline_context.PipelineContext()
    windowing_proto = windowing.to_runner_api(context)
    return cls.byte_array_to_json_string(
        beam_runner_api_pb2.MessageWithComponents(
            components=context.to_runner_api(),
            windowing_strategy=windowing_proto).SerializeToString())

  @classmethod
  def deserialize_windowing_strategy(cls, serialized_data):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners import pipeline_context
    from apache_beam.portability.api import beam_runner_api_pb2
    from apache_beam.transforms.core import Windowing
    proto = beam_runner_api_pb2.MessageWithComponents()
    proto.ParseFromString(cls.json_string_to_byte_array(serialized_data))
    return Windowing.from_runner_api(
        proto.windowing_strategy,
        pipeline_context.PipelineContext(proto.components))

  @staticmethod
  def byte_array_to_json_string(raw_bytes):
    """Implements org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString."""
    return urllib.quote(raw_bytes)

  @staticmethod
  def json_string_to_byte_array(encoded_string):
    """Implements org.apache.beam.sdk.util.StringUtils.jsonStringToByteArray."""
    return urllib.unquote(encoded_string)


class DataflowPipelineResult(PipelineResult):
  """Represents the state of a pipeline run on the Dataflow service."""

  def __init__(self, job, runner):
    """Initialize a new DataflowPipelineResult instance.

    Args:
      job: Job message from the Dataflow API. Could be :data:`None` if a job
        request was not sent to Dataflow service (e.g. template jobs).
      runner: DataflowRunner instance.
    """
    self._job = job
    self._runner = runner
    self.metric_results = None

  def _update_job(self):
    # We need the job id to be able to update job information. There is no need
    # to update the job if we are in a known terminal state.
    if self.has_job and not self._is_in_terminal_state():
      self._job = self._runner.dataflow_client.get_job(self.job_id())

  def job_id(self):
    return self._job.id

  def metrics(self):
    return self.metric_results

  @property
  def has_job(self):
    return self._job is not None

  @property
  def state(self):
    """Return the current state of the remote job.

    Returns:
      A PipelineState object.
    """
    if not self.has_job:
      return PipelineState.UNKNOWN

    self._update_job()

    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    # TODO: Move this table to a another location.
    # Ordered by the enum values.
    api_jobstate_map = {
        values_enum.JOB_STATE_UNKNOWN: PipelineState.UNKNOWN,
        values_enum.JOB_STATE_STOPPED: PipelineState.STOPPED,
        values_enum.JOB_STATE_RUNNING: PipelineState.RUNNING,
        values_enum.JOB_STATE_DONE: PipelineState.DONE,
        values_enum.JOB_STATE_FAILED: PipelineState.FAILED,
        values_enum.JOB_STATE_CANCELLED: PipelineState.CANCELLED,
        values_enum.JOB_STATE_UPDATED: PipelineState.UPDATED,
        values_enum.JOB_STATE_DRAINING: PipelineState.DRAINING,
        values_enum.JOB_STATE_DRAINED: PipelineState.DRAINED,
        values_enum.JOB_STATE_PENDING: PipelineState.PENDING,
        values_enum.JOB_STATE_CANCELLING: PipelineState.CANCELLING,
    }

    return (api_jobstate_map[self._job.currentState] if self._job.currentState
            else PipelineState.UNKNOWN)

  def _is_in_terminal_state(self):
    if not self.has_job:
      return True

    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum
    return self._job.currentState in [
        values_enum.JOB_STATE_STOPPED, values_enum.JOB_STATE_DONE,
        values_enum.JOB_STATE_FAILED, values_enum.JOB_STATE_CANCELLED,
        values_enum.JOB_STATE_UPDATED, values_enum.JOB_STATE_DRAINED]

  def wait_until_finish(self, duration=None):
    if not self._is_in_terminal_state():
      if not self.has_job:
        raise IOError('Failed to get the Dataflow job id.')

      thread = threading.Thread(
          target=DataflowRunner.poll_for_job_completion,
          args=(self._runner, self, duration))

      # Mark the thread as a daemon thread so a keyboard interrupt on the main
      # thread will terminate everything. This is also the reason we will not
      # use thread.join() to wait for the polling thread.
      thread.daemon = True
      thread.start()
      while thread.isAlive():
        time.sleep(5.0)

      # TODO: Merge the termination code in poll_for_job_completion and
      # _is_in_terminal_state.
      terminated = self._is_in_terminal_state()
      assert duration or terminated, (
          'Job did not reach to a terminal state after waiting indefinitely.')

      if terminated and self.state != PipelineState.DONE:
        # TODO(BEAM-1290): Consider converting this to an error log based on
        # theresolution of the issue.
        raise DataflowRuntimeException(
            'Dataflow pipeline failed. State: %s, Error:\n%s' %
            (self.state, getattr(self._runner, 'last_error_msg', None)), self)
    return self.state

  def cancel(self):
    if not self.has_job:
      raise IOError('Failed to get the Dataflow job id.')

    self._update_job()

    if self._is_in_terminal_state():
      logging.warning(
          'Cancel failed because job %s is already terminated in state %s.',
          self.job_id(), self.state)
    else:
      if not self._runner.dataflow_client.modify_job_state(
          self.job_id(), 'JOB_STATE_CANCELLED'):
        cancel_failed_message = (
            'Failed to cancel job %s, please go to the Developers Console to '
            'cancel it manually.') % self.job_id()
        logging.error(cancel_failed_message)
        raise DataflowRuntimeException(cancel_failed_message, self)

    return self.state

  def __str__(self):
    return '<%s %s %s>' % (
        self.__class__.__name__,
        self.job_id(),
        self.state)

  def __repr__(self):
    return '<%s %s at %s>' % (self.__class__.__name__, self._job, hex(id(self)))


class DataflowRuntimeException(Exception):
  """Indicates an error has occurred in running this pipeline."""

  def __init__(self, msg, result):
    super(DataflowRuntimeException, self).__init__(msg)
    self.result = result
