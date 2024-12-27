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
import dataclasses
import typing as t

import apache_beam as beam
from apache_beam import typehints
from apache_beam.io.iobase import SourceBase
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.direct.direct_runner import _GroupAlsoByWindowDoFn
from apache_beam.transforms import ptransform
from apache_beam.transforms.window import GlobalWindows

K = t.TypeVar("K")
V = t.TypeVar("V")


@dataclasses.dataclass
class _Create(beam.PTransform):
  values: t.Tuple[t.Any]

  def expand(self, input_or_inputs):
    return beam.pvalue.PCollection.from_(input_or_inputs)

  def get_windowing(self, inputs: t.Any) -> beam.Windowing:
    return beam.Windowing(GlobalWindows())


@typehints.with_input_types(K)
@typehints.with_output_types(K)
class _Reshuffle(beam.PTransform):
  def expand(self, input_or_inputs):
    return beam.pvalue.PCollection.from_(input_or_inputs)


@dataclasses.dataclass
class _Read(beam.PTransform):
  source: SourceBase

  def expand(self, input_or_inputs):
    return beam.pvalue.PCollection.from_(input_or_inputs)


@typehints.with_input_types(t.Tuple[K, V])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupByKeyOnly(beam.PTransform):
  def expand(self, input_or_inputs):
    return beam.pvalue.PCollection.from_(input_or_inputs)

  def infer_output_type(self, input_type):

    key_type, value_type = typehints.trivial_inference.key_value_types(
      input_type
    )
    return typehints.KV[key_type, typehints.Iterable[value_type]]


@typehints.with_input_types(t.Tuple[K, t.Iterable[V]])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupAlsoByWindow(beam.ParDo):
  def __init__(self, windowing):
    super().__init__(_GroupAlsoByWindowDoFn(windowing))
    self.windowing = windowing

  def expand(self, input_or_inputs):
    return beam.pvalue.PCollection.from_(input_or_inputs)


@typehints.with_input_types(t.Tuple[K, V])
@typehints.with_output_types(t.Tuple[K, t.Iterable[V]])
class _GroupByKey(beam.PTransform):
  def expand(self, input_or_inputs):
    return (
        input_or_inputs
        | "ReifyWindows" >> beam.ParDo(beam.GroupByKey.ReifyWindows())
        | "GroupByKey" >> _GroupByKeyOnly()
        | "GroupByWindow" >> _GroupAlsoByWindow(input_or_inputs.windowing))


class _Flatten(beam.PTransform):
  def expand(self, input_or_inputs):
    if isinstance(input_or_inputs, beam.PCollection):
      # NOTE(cisaacstern): I needed this to avoid
      #   `TypeError: 'PCollection' object is not iterable`
      # being raised by `all(...)` call below for single-element flattens, i.e.,
      #   `(pcoll, ) | beam.Flatten() | ...`
      is_bounded = input_or_inputs.is_bounded
    else:
      is_bounded = all(pcoll.is_bounded for pcoll in input_or_inputs)
    return beam.pvalue.PCollection(self.pipeline, is_bounded=is_bounded)


def dask_overrides() -> t.List[PTransformOverride]:
  class CreateOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
      return applied_ptransform.transform.__class__ == beam.Create

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
      return _Create(t.cast(beam.Create, applied_ptransform.transform).values)

  class ReshuffleOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
      return applied_ptransform.transform.__class__ == beam.Reshuffle

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
      return _Reshuffle()

  class ReadOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
      return applied_ptransform.transform.__class__ == beam.io.Read

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
      return _Read(t.cast(beam.io.Read, applied_ptransform.transform).source)

  class GroupByKeyOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
      return applied_ptransform.transform.__class__ == beam.GroupByKey

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
      return _GroupByKey()

  class FlattenOverride(PTransformOverride):
    def matches(self, applied_ptransform: AppliedPTransform) -> bool:
      return applied_ptransform.transform.__class__ == beam.Flatten

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform: AppliedPTransform) -> ptransform.PTransform:
      return _Flatten()

  return [
      CreateOverride(),
      ReshuffleOverride(),
      ReadOverride(),
      GroupByKeyOverride(),
      FlattenOverride(),
  ]
