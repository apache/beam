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

import base64
import logging
import threading
import time
import traceback

from apache_beam import coders
from apache_beam import pvalue
from apache_beam.internal import json_value
from apache_beam.internal import pickler
from apache_beam.pvalue import PCollectionView
from apache_beam.runners.dataflow.dataflow_metrics import DataflowMetrics
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PValueCache
from apache_beam.transforms.display import DisplayData
from apache_beam.typehints import typehints
from apache_beam.utils import names
from apache_beam.utils.names import PropertyNames
from apache_beam.utils.names import TransformNames
from apache_beam.utils.pipeline_options import StandardOptions
from apache_beam.internal.clients import dataflow as dataflow_api


class DataflowRunner(PipelineRunner):
  """A runner that creates job graphs and submits them for remote execution.

  Every execution of the run() method will submit an independent job for
  remote execution that consists of the nodes reachable from the passed in
  node argument or entire graph if node is None. The run() method returns
  after the service created the job and  will not wait for the job to finish
  if blocking is set to False.
  """

  # Environment version information. It is passed to the service during a
  # a job submission and is used by the service to establish what features
  # are expected by the workers.
  BATCH_ENVIRONMENT_MAJOR_VERSION = '5'
  STREAMING_ENVIRONMENT_MAJOR_VERSION = '0'

  def __init__(self, cache=None):
    # Cache of CloudWorkflowStep protos generated while the runner
    # "executes" a pipeline.
    self._cache = cache if cache is not None else PValueCache()
    self._unique_step_id = 0

  def _get_unique_step_name(self):
    self._unique_step_id += 1
    return 's%s' % self._unique_step_id

  @staticmethod
  def poll_for_job_completion(runner, result):
    """Polls for the specified job to finish running (successfully or not)."""
    last_message_time = None
    last_message_id = None

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
      else:
        return 0

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
          if last_message_id is not None and m.id == last_message_id:
            # Skip the first message if it is the last message we got in the
            # previous round. This can happen because we use the
            # last_message_time as a parameter of the query for new messages.
            continue
          last_message_time = m.time
          last_message_id = m.id
          # Skip empty messages.
          if m.messageImportance is None:
            continue
          logging.info(
              '%s: %s: %s: %s', m.id, m.time, m.messageImportance,
              m.messageText)
          if str(m.messageImportance) == 'JOB_MESSAGE_ERROR':
            if rank_error(m.messageText) >= last_error_rank:
              last_error_rank = rank_error(m.messageText)
              last_error_msg = m.messageText
        if not page_token:
          break

    result._job = response
    runner.last_error_msg = last_error_msg

  def run(self, pipeline):
    """Remotely executes entire pipeline or parts reachable from node."""
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.internal import apiclient
    self.job = apiclient.Job(pipeline.options)

    # The superclass's run will trigger a traversal of all reachable nodes.
    super(DataflowRunner, self).run(pipeline)

    standard_options = pipeline.options.view_as(StandardOptions)
    if standard_options.streaming:
      job_version = DataflowRunner.STREAMING_ENVIRONMENT_MAJOR_VERSION
    else:
      job_version = DataflowRunner.BATCH_ENVIRONMENT_MAJOR_VERSION

    # Get a Dataflow API client and set its options
    self.dataflow_client = apiclient.DataflowApplicationClient(
        pipeline.options, job_version)

    # Create the job
    return DataflowPipelineResult(
        self.dataflow_client.create_job(self.job), self)

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
    else:
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
        and transform_node.outputs[0].element_type is not None):
      # TODO(robertwb): Handle type hints for multi-output transforms.
      element_type = transform_node.outputs[0].element_type
    else:
      # TODO(silviuc): Remove this branch (and assert) when typehints are
      # propagated everywhere. Returning an 'Any' as type hint will trigger
      # usage of the fallback coder (i.e., cPickler).
      element_type = typehints.Any
    if window_value:
      window_coder = (
          transform_node.outputs[0].windowing.windowfn.get_window_coder())
    else:
      window_coder = None
    return self._get_typehint_based_encoding(
        element_type, window_coder=window_coder)

  def _add_step(self, step_kind, step_label, transform_node, side_tags=()):
    """Creates a Step object and adds it to the cache."""
    # Import here to avoid adding the dependency for local running scenarios.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.internal import apiclient
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

  def run_Create(self, transform_node):
    transform = transform_node.transform
    step = self._add_step(TransformNames.CREATE_PCOLLECTION,
                          transform_node.full_label, transform_node)
    # TODO(silviuc): Eventually use a coder based on typecoders.
    # Note that we base64-encode values here so that the service will accept
    # the values.
    element_coder = coders.PickleCoder()
    step.add_property(
        PropertyNames.ELEMENT,
        [base64.b64encode(element_coder.encode(v))
         for v in transform.value])
    # The service expects a WindowedValueCoder here, so we wrap the actual
    # encoding in a WindowedValueCoder.
    step.encoding = self._get_cloud_encoding(
        coders.WindowedValueCoder(element_coder))
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])

  def run_CreatePCollectionView(self, transform_node):
    step = self._add_step(TransformNames.COLLECTION_TO_SINGLETON,
                          transform_node.full_label, transform_node)
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    step.encoding = self._get_side_input_encoding(input_step.encoding)
    step.add_property(
        PropertyNames.OUTPUT_INFO,
        [{PropertyNames.USER_NAME: (
            '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
          PropertyNames.ENCODING: step.encoding,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])

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
    step.add_property(PropertyNames.SERIALIZED_FN, pickler.dumps(windowing))

  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    input_tag = transform_node.inputs[0].tag
    input_step = self._cache.get_pvalue(transform_node.inputs[0])

    # Attach side inputs.
    si_dict = {}
    # We must call self._cache.get_pvalue exactly once due to refcounting.
    si_labels = {}
    for side_pval in transform_node.side_inputs:
      si_labels[side_pval] = self._cache.get_pvalue(side_pval).step_name
    lookup_label = lambda side_pval: si_labels[side_pval]
    for side_pval in transform_node.side_inputs:
      assert isinstance(side_pval, PCollectionView)
      si_label = lookup_label(side_pval)
      si_dict[si_label] = {
          '@type': 'OutputReference',
          PropertyNames.STEP_NAME: si_label,
          PropertyNames.OUTPUT_NAME: PropertyNames.OUT}

    # Now create the step for the ParDo transform being handled.
    step = self._add_step(
        TransformNames.DO, transform_node.full_label, transform_node,
        transform_node.transform.side_output_tags)
    fn_data = self._pardo_fn_data(transform_node, lookup_label)
    step.add_property(PropertyNames.SERIALIZED_FN, pickler.dumps(fn_data))
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    # Add side inputs if any.
    step.add_property(PropertyNames.NON_PARALLEL_INPUTS, si_dict)

    # Generate description for main output and side outputs. The output names
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
    for side_tag in transform.side_output_tags:
      # The assumption here is that side outputs will have the same typehint
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
    accumulator_encoding = self._get_encoded_output_coder(transform_node,
                                                          window_value=False)
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
        raise ValueError('PubSubSource is currently available for use only in '
                         'streaming pipelines.')
      step.add_property(PropertyNames.PUBSUB_TOPIC, transform.source.topic)
      if transform.source.subscription:
        step.add_property(PropertyNames.PUBSUB_SUBSCRIPTION,
                          transform.source.topic)
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
    coder = coders.WindowedValueCoder(transform._infer_output_coder())  # pylint: disable=protected-access

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
        raise ValueError('PubSubSink is currently available for use only in '
                         'streaming pipelines.')
      step.add_property(PropertyNames.PUBSUB_TOPIC, transform.sink.topic)
    else:
      raise ValueError(
          'Sink %r has unexpected format %s.' % (
              transform.sink, transform.sink.format))
    step.add_property(PropertyNames.FORMAT, transform.sink.format)

    # Wrap coder in WindowedValueCoder: this is necessary for proper encoding
    # for size estimation.
    coder = coders.WindowedValueCoder(transform.sink.coder)
    step.encoding = self._get_cloud_encoding(coder)
    step.add_property(PropertyNames.ENCODING, step.encoding)
    step.add_property(
        PropertyNames.PARALLEL_INPUT,
        {'@type': 'OutputReference',
         PropertyNames.STEP_NAME: input_step.proto.name,
         PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})


class DataflowPipelineResult(PipelineResult):
  """Represents the state of a pipeline run on the Dataflow service."""

  def __init__(self, job, runner):
    """Job is a Job message from the Dataflow API."""
    self._job = job
    self._runner = runner

  def job_id(self):
    return self._job.id

  def metrics(self):
    return DataflowMetrics()

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

    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum
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
    }

    return (api_jobstate_map[self._job.currentState] if self._job.currentState
            else PipelineState.UNKNOWN)

  def _is_in_terminal_state(self):
    if not self.has_job:
      return True

    return self.state in [
        PipelineState.STOPPED, PipelineState.DONE, PipelineState.FAILED,
        PipelineState.CANCELLED, PipelineState.DRAINED]

  def wait_until_finish(self, duration=None):
    if not self._is_in_terminal_state():
      if not self.has_job:
        raise IOError('Failed to get the Dataflow job id.')
      if duration:
        raise NotImplementedError(
            'DataflowRunner does not support duration argument.')

      thread = threading.Thread(
          target=DataflowRunner.poll_for_job_completion,
          args=(self._runner, self))

      # Mark the thread as a daemon thread so a keyboard interrupt on the main
      # thread will terminate everything. This is also the reason we will not
      # use thread.join() to wait for the polling thread.
      thread.daemon = True
      thread.start()
      while thread.isAlive():
        time.sleep(5.0)
      if self.state != PipelineState.DONE:
        # TODO(BEAM-1290): Consider converting this to an error log based on the
        # resolution of the issue.
        raise DataflowRuntimeException(
            'Dataflow pipeline failed. State: %s, Error:\n%s' %
            (self.state, getattr(self._runner, 'last_error_msg', None)), self)
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
