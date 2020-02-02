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

  def get_replacement_transform(self, ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import PTransform
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
    if (isinstance(applied_ptransform.transform, Read)
        and not getattr(applied_ptransform.transform, 'override', False)):
      if isinstance(applied_ptransform.transform.source, BoundedSource):
        return True
    return False

  def get_replacement_transform(self, ptransform):
    from apache_beam import pvalue
    from apache_beam.io import iobase
    class Read(iobase.Read):
      override = True
      def expand(self, pbegin):
        return pvalue.PCollection(
            self.pipeline, is_bounded=self.source.is_bounded())
    return Read(ptransform.source).with_output_types(
        ptransform.get_type_hints().simple_output_type('Read'))


class JrhReadPTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Read(BoundedSource)``"""

  def matches(self, applied_ptransform):
    from apache_beam.io import Read
    from apache_beam.io.iobase import BoundedSource
    return (isinstance(applied_ptransform.transform, Read)
            and isinstance(applied_ptransform.transform.source, BoundedSource))

  def get_replacement_transform(self, ptransform):
    from apache_beam.io import Read
    from apache_beam.transforms import core
    from apache_beam.transforms import util
    # Make this a local to narrow what's captured in the closure.
    source = ptransform.source

    class JrhRead(core.PTransform):
      def expand(self, pbegin):
        return (
            pbegin
            | core.Impulse()
            | 'Split' >> core.FlatMap(lambda _: source.split(
                Read.get_desired_chunk_size(source.estimate_size())))
            | util.Reshuffle()
            | 'ReadSplits' >> core.FlatMap(lambda split: split.source.read(
                split.source.get_range_tracker(
                    split.start_position, split.stop_position))))

    return JrhRead().with_output_types(
        ptransform.get_type_hints().simple_output_type('Read'))
