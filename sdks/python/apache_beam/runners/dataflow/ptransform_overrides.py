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

from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pipeline import PTransformOverride


class StreamingWriteToPubSubOverride(PTransformOverride):
  """Override WriteToPubSub for streaming mode in DataflowRunner.

  This override provides streaming-specific optimizations for WriteToPubSub
  when running on DataflowRunner in streaming mode. For batch mode, the
  default WriteToPubSub implementation is used.
  """
  def matches(self, applied_ptransform):
    from apache_beam.io.gcp import pubsub as beam_pubsub
    return isinstance(applied_ptransform.transform, beam_pubsub.WriteToPubSub)

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform):
    # Use the traditional Write(sink) pattern for DataflowRunner streaming mode
    from apache_beam.io.iobase import Write
    return Write(applied_ptransform.transform._sink)


def get_dataflow_transform_overrides(pipeline_options):
  """Returns DataflowRunner-specific transform overrides.

  Args:
    pipeline_options: Pipeline options to determine which overrides to apply.

  Returns:
    List of PTransformOverride objects for DataflowRunner.
  """
  overrides = []

  # Only add streaming-specific overrides when in streaming mode
  if pipeline_options.view_as(StandardOptions).streaming:
    # Add PubSub streaming override (placeholder for future optimizations)
    overrides.append(StreamingWriteToPubSubOverride())

  return overrides


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
