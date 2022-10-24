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

"""Transform Beam PTransforms into Dask Bag operations.

A minimum set of operation substitutions, to adap Beam's PTransform model
to Dask Bag functions.

TODO(alxr): Translate ops from https://docs.dask.org/en/latest/bag-api.html.
"""
import abc
import dataclasses
import typing as t
from dataclasses import field

import dask.bag as db

import apache_beam
from apache_beam import TaggedOutput
from apache_beam import DoFn
from apache_beam.internal import util
from apache_beam.pipeline import AppliedPTransform
from apache_beam.runners.common import DoFnContext
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import Receiver
from apache_beam.runners.common import _OutputHandler
from apache_beam.runners.common import DoFnInvoker
from apache_beam.runners.dask.overrides import _Create
from apache_beam.runners.dask.overrides import _Flatten
from apache_beam.runners.dask.overrides import _GroupByKeyOnly
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue

OpInput = t.Union[db.Bag, t.Sequence[db.Bag], None]
PCollVal = t.Union[WindowedValue, t.Any]


def get_windowed_value(item: t.Any, window_fn: WindowFn) -> WindowedValue:
  if isinstance(item, TaggedOutput):
    item = item.value

  if isinstance(item, WindowedValue):
    windowed_value = item
  elif isinstance(item, TimestampedValue):
    assign_context = WindowFn.AssignContext(item.timestamp, item.value)
    windowed_value = WindowedValue(item.value, item.timestamp,
                                   tuple(window_fn.assign(assign_context)))
  else:
    windowed_value = WindowedValue(item, 0, (GlobalWindow(),))

  return windowed_value


@dataclasses.dataclass
class TaggingReceiver(Receiver):
  tag: str
  values: t.List[PCollVal]

  def receive(self, windowed_value: WindowedValue):
    if self.tag:
      output = TaggedOutput(self.tag, windowed_value)
    else:
      output = windowed_value
    self.values.append(output)


@dataclasses.dataclass
class OneReceiver(dict):
  values: t.List[PCollVal] = field(
    default_factory=list
  )

  def __missing__(self, key):
    if key not in self:
      self[key] = TaggingReceiver(key, self.values)
    return self[key]


@dataclasses.dataclass
class DaskBagOp(abc.ABC):
  applied: AppliedPTransform

  @property
  def transform(self):
    return self.applied.transform

  @abc.abstractmethod
  def apply(self, input_bag: OpInput) -> db.Bag:
    pass


class NoOp(DaskBagOp):
  def apply(self, input_bag: OpInput) -> db.Bag:
    return input_bag


class Create(DaskBagOp):
  def apply(self, input_bag: OpInput) -> db.Bag:
    assert input_bag is None, 'Create expects no input!'
    original_transform = t.cast(_Create, self.transform)
    items = original_transform.values
    return db.from_sequence(items)


class ParDo(DaskBagOp):

  def apply(self, input_bag: OpInput) -> db.Bag:
    transform = t.cast(apache_beam.ParDo, self.transform)

    label = transform.label
    map_fn = transform.fn
    args, kwargs = transform.raw_side_inputs
    main_input = next(iter(self.applied.main_inputs.values()))
    window_fn = main_input.windowing.windowfn if hasattr(main_input, "windowing") else None

    context = DoFnContext(label, state=None)
    bundle_finalizer_param = DoFn.BundleFinalizerParam()
    do_fn_signature = DoFnSignature(map_fn)

    tagged_receivers = OneReceiver()

    output_processor = _OutputHandler(
      window_fn=window_fn,
      main_receivers=tagged_receivers[None],
      tagged_receivers=tagged_receivers,
      per_element_output_counter=None,
      output_batch_converter=None,
      process_yields_batches=False,
      process_batch_yields_elements=False
    )

    do_fn_invoker = DoFnInvoker.create_invoker(
      do_fn_signature,
      output_processor,
      context,
      None,
      args,
      kwargs,
      user_state_context=None,
      bundle_finalizer_param=bundle_finalizer_param)

    def apply_dofn_to_bundle(items):
      do_fn_invoker.invoke_setup()
      do_fn_invoker.invoke_start_bundle()

      results = [do_fn_invoker.invoke_process(it) for it in items]
      results.extend(tagged_receivers.values)

      do_fn_invoker.invoke_finish_bundle()
      do_fn_invoker.invoke_teardown()

      return results

    return input_bag.map(get_windowed_value, window_fn).map_partitions(apply_dofn_to_bundle).flatten()


class Map(DaskBagOp):
  def apply(self, input_bag: OpInput) -> db.Bag:
    transform = t.cast(apache_beam.Map, self.transform)
    args, kwargs = util.insert_values_in_args(
      transform.args, transform.kwargs, transform.side_inputs)
    return input_bag.map(transform.fn.process, *args, **kwargs)


class GroupByKey(DaskBagOp):
  def apply(self, input_bag: OpInput) -> db.Bag:
    def key(item):
      return item[0]

    def value(item):
      k, v = item
      return k, [elm[1] for elm in v]

    return input_bag.groupby(key).map(value)


class Flatten(DaskBagOp):
  def apply(self, input_bag: OpInput) -> db.Bag:
    assert type(input_bag) is list, 'Must take a sequence of bags!'
    return db.concat(input_bag)


TRANSLATIONS = {
    _Create: Create,
  apache_beam.ParDo: ParDo,
  apache_beam.Map: Map,
  _GroupByKeyOnly: GroupByKey,
  _Flatten: Flatten,
}
