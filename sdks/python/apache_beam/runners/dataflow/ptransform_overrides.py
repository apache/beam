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

"""Ptransform overrides for DataflowRunner."""

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pipeline import PTransformOverride


class CreatePTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Create`` in streaming mode."""
  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import Create
    from apache_beam.runners.dataflow.internal import apiclient

    if isinstance(applied_ptransform.transform, Create):
      return not apiclient._use_fnapi(
          applied_ptransform.outputs[None].pipeline._options)
    else:
      return False

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import PTransform

    ptransform = applied_ptransform.transform

    # Return a wrapper rather than ptransform.as_read() directly to
    # ensure backwards compatibility of the pipeline structure.
    class LegacyCreate(PTransform):
      def expand(self, pbegin):
        return pbegin | ptransform.as_read()

    return LegacyCreate().with_output_types(ptransform.get_output_type())


class ReadPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Read(BoundedSource)``"""
  def matches(self, applied_ptransform):
    from apache_beam.io import Read
    from apache_beam.io.iobase import BoundedSource
    # Only overrides Read(BoundedSource) transform
    if (isinstance(applied_ptransform.transform, Read) and
        not getattr(applied_ptransform.transform, 'override', False)):
      if isinstance(applied_ptransform.transform.source, BoundedSource):
        return True
    return False

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform):

    from apache_beam import pvalue
    from apache_beam.io import iobase

    transform = applied_ptransform.transform

    class Read(iobase.Read):
      override = True

      def expand(self, pbegin):
        return pvalue.PCollection(
            self.pipeline, is_bounded=self.source.is_bounded())

    return Read(transform.source).with_output_types(
        transform.get_type_hints().simple_output_type('Read'))


class JrhReadPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Read(BoundedSource)``"""
  def matches(self, applied_ptransform):
    from apache_beam.io import Read
    from apache_beam.io.iobase import BoundedSource
    return (
        isinstance(applied_ptransform.transform, Read) and
        isinstance(applied_ptransform.transform.source, BoundedSource))

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform):
    from apache_beam.io import Read
    from apache_beam.transforms import core
    from apache_beam.transforms import util
    # Make this a local to narrow what's captured in the closure.
    source = applied_ptransform.transform.source

    class JrhRead(core.PTransform):
      def expand(self, pbegin):
        return (
            pbegin
            | core.Impulse()
            | 'Split' >> core.FlatMap(
                lambda _: source.split(
                    Read.get_desired_chunk_size(source.estimate_size())))
            | util.Reshuffle()
            | 'ReadSplits' >> core.FlatMap(
                lambda split: split.source.read(
                    split.source.get_range_tracker(
                        split.start_position, split.stop_position))))

    return JrhRead().with_output_types(
        applied_ptransform.transform.get_type_hints().simple_output_type(
            'Read'))


class CombineValuesPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``CombineValues``.

  The DataflowRunner expects that the CombineValues PTransform acts as a
  primitive. So this override replaces the CombineValues with a primitive.
  """
  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import CombineValues

    if isinstance(applied_ptransform.transform, CombineValues):
      self.transform = applied_ptransform.transform
      return True
    return False

  def get_replacement_transform(self, ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import PTransform
    from apache_beam.pvalue import PCollection

    # The DataflowRunner still needs access to the CombineValues members to
    # generate a V1B3 proto representation, so we remember the transform from
    # the matches method and forward it here.
    class CombineValuesReplacement(PTransform):
      def __init__(self, transform):
        self.transform = transform

      def expand(self, pcoll):
        return PCollection.from_(pcoll)

    return CombineValuesReplacement(self.transform)


class NativeReadPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Read`` using native sources.

  The DataflowRunner expects that the Read PTransform using native sources act
  as a primitive. So this override replaces the Read with a primitive.
  """
  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.io import Read

    # Consider the native Read to be a primitive for Dataflow by replacing.
    return (
        isinstance(applied_ptransform.transform, Read) and
        not getattr(applied_ptransform.transform, 'override', False) and
        hasattr(applied_ptransform.transform.source, 'format'))

  def get_replacement_transform(self, ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import pvalue
    from apache_beam.io import iobase

    # This is purposely subclassed from the Read transform to take advantage of
    # the existing windowing, typing, and display data.
    class Read(iobase.Read):
      override = True

      def expand(self, pbegin):
        return pvalue.PCollection.from_(pbegin)

    # Use the source's coder type hint as this replacement's output. Otherwise,
    # the typing information is not properly forwarded to the DataflowRunner and
    # will choose the incorrect coder for this transform.
    return Read(ptransform.source).with_output_types(
        ptransform.source.coder.to_type_hint())


class WriteToBigQueryPTransformOverride(PTransformOverride):
  def __init__(self, pipeline, options):
    super(WriteToBigQueryPTransformOverride, self).__init__()
    self.options = options
    self.outputs = []

    self._check_bq_outputs(pipeline)

  def _check_bq_outputs(self, pipeline):
    """Checks that there are no consumers if the transform will be replaced.

    The WriteToBigQuery replacement is the native BigQuerySink which has an
    output of a PDone. The original transform, however, returns a dict. The user
    may be inadvertantly using the dict output which will have no side-effects
    or fail pipeline construction esoterically. This checks the outputs and
    gives a user-friendsly error.
    """
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.pipeline import PipelineVisitor
    from apache_beam.io import WriteToBigQuery

    # First, retrieve all the outpts from all the WriteToBigQuery transforms
    # that will be replaced. Later, we will use these to make sure no one
    # consumes these.
    class GetWriteToBqOutputsVisitor(PipelineVisitor):
      def __init__(self, matches):
        self.matches = matches
        self.outputs = set()

      def enter_composite_transform(self, transform_node):
        # Only add outputs that are going to be replaced.
        if self.matches(transform_node):
          self.outputs.update(set(transform_node.outputs.values()))

    outputs_visitor = GetWriteToBqOutputsVisitor(self.matches)
    pipeline.visit(outputs_visitor)

    # Finally, verify that there are no consumers to the previously found
    # outputs.
    class VerifyWriteToBqOutputsVisitor(PipelineVisitor):
      def __init__(self, outputs):
        self.outputs = outputs

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        # Internal consumers of the outputs we're overriding are expected.
        # We only error out on non-internal consumers.
        if ('BigQueryBatchFileLoads' not in transform_node.full_label and
            [o for o in self.outputs if o in transform_node.inputs]):
          raise ValueError(
              'WriteToBigQuery was being replaced with the native '
              'BigQuerySink, but the transform "{}" has an input which will be '
              'replaced with a PDone. To fix, please remove all transforms '
              'that read from any WriteToBigQuery transforms.'.format(
                  transform_node.full_label))

    pipeline.visit(VerifyWriteToBqOutputsVisitor(outputs_visitor.outputs))

  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import io
    transform = applied_ptransform.transform
    if (not isinstance(transform, io.WriteToBigQuery) or
        getattr(transform, 'override', False)):
      return False

    experiments = self.options.view_as(DebugOptions).experiments or []
    if 'use_legacy_bq_sink' not in experiments:
      return False

    if transform.schema == io.gcp.bigquery.SCHEMA_AUTODETECT:
      raise RuntimeError(
          'Schema auto-detection is not supported on the native sink')

    # The replacement is only valid for Batch.
    standard_options = self.options.view_as(StandardOptions)
    if standard_options.streaming:
      if transform.write_disposition == io.BigQueryDisposition.WRITE_TRUNCATE:
        raise RuntimeError('Can not use write truncation mode in streaming')
      return False

    self.outputs = list(applied_ptransform.outputs.keys())
    return True

  def get_replacement_transform(self, ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import io

    class WriteToBigQuery(io.WriteToBigQuery):
      override = True

      def __init__(self, transform, outputs):
        self.transform = transform
        self.outputs = outputs

      def __getattr__(self, name):
        """Returns the given attribute from the parent.

        This allows this transform to act like a WriteToBigQuery transform
        without having to construct a new WriteToBigQuery transform.
        """
        return self.transform.__getattribute__(name)

      def expand(self, pcoll):
        from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
        import json

        schema = None
        if self.schema:
          schema = parse_table_schema_from_json(json.dumps(self.schema))

        out = pcoll | io.Write(
            io.BigQuerySink(
                self.table_reference.tableId,
                self.table_reference.datasetId,
                self.table_reference.projectId,
                schema,
                self.create_disposition,
                self.write_disposition,
                kms_key=self.kms_key))

        # The WriteToBigQuery can have different outputs depending on if it's
        # Batch or Streaming. This retrieved the output keys from the node and
        # is replacing them here to be consistent.
        return {key: out for key in self.outputs}

    return WriteToBigQuery(ptransform, self.outputs)
