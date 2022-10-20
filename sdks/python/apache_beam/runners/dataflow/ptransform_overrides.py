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

from apache_beam.options.pipeline_options import GoogleCloudOptions
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


class GroupIntoBatchesWithShardedKeyPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``GroupIntoBatches.WithShardedKey``.

  This override simply returns the original transform but additionally records
  the output PCollection in order to append required step properties during
  graph translation.
  """
  def __init__(self, dataflow_runner, options):
    self.dataflow_runner = dataflow_runner
    self.options = options

  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import util

    transform = applied_ptransform.transform

    if not isinstance(transform, util.GroupIntoBatches.WithShardedKey):
      return False

    # The replacement is only valid for portable Streaming Engine jobs with
    # runner v2.
    standard_options = self.options.view_as(StandardOptions)
    if not standard_options.streaming:
      return False
    google_cloud_options = self.options.view_as(GoogleCloudOptions)
    if not google_cloud_options.enable_streaming_engine:
      raise ValueError(
          'Runner determined sharding not available in Dataflow for '
          'GroupIntoBatches for non-Streaming-Engine jobs. In order to use '
          'runner determined sharding, please use '
          '--streaming --enable_streaming_engine --experiments=use_runner_v2')

    from apache_beam.runners.dataflow.internal import apiclient
    if not apiclient._use_unified_worker(self.options):
      raise ValueError(
          'Runner determined sharding not available in Dataflow for '
          'GroupIntoBatches for jobs not using Runner V2. In order to use '
          'runner determined sharding, please use '
          '--streaming --enable_streaming_engine --experiments=use_runner_v2')

    self.dataflow_runner.add_pcoll_with_auto_sharding(applied_ptransform)
    return True

  def get_replacement_transform_for_applied_ptransform(self, ptransform):
    return ptransform.transform
