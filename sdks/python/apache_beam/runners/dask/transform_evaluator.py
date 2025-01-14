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
import math
import typing as t
from dataclasses import field

import apache_beam
import dask.bag as db
from apache_beam import DoFn
from apache_beam import TaggedOutput
from apache_beam.pipeline import AppliedPTransform
from apache_beam.runners.common import DoFnContext
from apache_beam.runners.common import DoFnInvoker
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import Receiver
from apache_beam.runners.common import _OutputHandler
from apache_beam.runners.dask.overrides import _Create
from apache_beam.runners.dask.overrides import _Flatten
from apache_beam.runners.dask.overrides import _GroupByKeyOnly
from apache_beam.transforms.sideinputs import SideInputMap
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils.windowed_value import WindowedValue

# Inputs to DaskOps.
OpInput = t.Union[db.Bag, t.Sequence[db.Bag], None]
OpSide = t.Optional[t.Sequence[SideInputMap]]

# Value types for PCollections (possibly Windowed Values).
PCollVal = t.Union[WindowedValue, t.Any]


def get_windowed_value(item: t.Any, window_fn: WindowFn) -> WindowedValue:
  """Wraps a value (item) inside a Window."""
  if isinstance(item, TaggedOutput):
    item = item.value

  if isinstance(item, WindowedValue):
    windowed_value = item
  elif isinstance(item, TimestampedValue):
    assign_context = WindowFn.AssignContext(item.timestamp, item.value)
    windowed_value = WindowedValue(
        item.value, item.timestamp, tuple(window_fn.assign(assign_context)))
  else:
    windowed_value = WindowedValue(item, 0, (GlobalWindow(), ))

  return windowed_value


def defenestrate(x):
  """Extracts the underlying item from a Window."""
  if isinstance(x, WindowedValue):
    return x.value
  return x


@dataclasses.dataclass
class DaskBagWindowedIterator:
  """Iterator for `apache_beam.transforms.sideinputs.SideInputMap`"""

  bag: db.Bag
  window_fn: WindowFn

  def __iter__(self):
    # FIXME(cisaacstern): list() is likely inefficient, since it presumably
    # materializes the full result before iterating over it. doing this for
    # now as a proof-of-concept. can we can generate results incrementally?
    for result in list(self.bag):
      yield get_windowed_value(result, self.window_fn)


@dataclasses.dataclass
class TaggingReceiver(Receiver):
  """A Receiver that handles tagged `WindowValue`s."""
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
  """A Receiver that tags value via dictionary lookup key."""
  values: t.List[PCollVal] = field(default_factory=list)

  def __missing__(self, key):
    if key not in self:
      self[key] = TaggingReceiver(key, self.values)
    return self[key]


@dataclasses.dataclass
class DaskBagOp(abc.ABC):
  """Abstract Base Class for all Dask-supported Operations.

  All DaskBagOps must support an `apply()` operation, which invokes the dask
  bag upon the previous op's input.

  Attributes
    applied: The underlying `AppliedPTransform` which holds the code for the
      target operation.
  """
  applied: AppliedPTransform

  @property
  def transform(self):
    return self.applied.transform

  @abc.abstractmethod
  def apply(self, input_bag: OpInput, side_inputs: OpSide = None) -> db.Bag:
    pass


class NoOp(DaskBagOp):
  """An identity on a dask bag: returns the input as-is."""
  def apply(self, input_bag: OpInput, side_inputs: OpSide = None) -> db.Bag:
    return input_bag


class Create(DaskBagOp):
  """The beginning of a Beam pipeline; the input must be `None`."""
  def apply(self, input_bag: OpInput, side_inputs: OpSide = None) -> db.Bag:
    assert input_bag is None, 'Create expects no input!'
    original_transform = t.cast(_Create, self.transform)
    items = original_transform.values
    return db.from_sequence(
        items,
        partition_size=max(
            1, math.ceil(math.sqrt(len(items)) / math.sqrt(100))))


def apply_dofn_to_bundle(
    items, do_fn_invoker_args, do_fn_invoker_kwargs, tagged_receivers):
  """Invokes a DoFn within a bundle, implemented as a Dask partition."""

  do_fn_invoker = DoFnInvoker.create_invoker(
      *do_fn_invoker_args, **do_fn_invoker_kwargs)

  do_fn_invoker.invoke_setup()
  do_fn_invoker.invoke_start_bundle()

  for it in items:
    do_fn_invoker.invoke_process(it)

  results = [v.value for v in tagged_receivers.values]

  do_fn_invoker.invoke_finish_bundle()
  do_fn_invoker.invoke_teardown()

  return results


class ParDo(DaskBagOp):
  """Apply a pure function in an embarrassingly-parallel way.

  This consumes a sequence of items and returns a sequence of items.
  """
  def apply(self, input_bag: db.Bag, side_inputs: OpSide = None) -> db.Bag:
    transform = t.cast(apache_beam.ParDo, self.transform)

    args, kwargs = transform.raw_side_inputs
    args = list(args)
    main_input = next(iter(self.applied.main_inputs.values()))
    window_fn = main_input.windowing.windowfn if hasattr(
        main_input, "windowing") else None

    tagged_receivers = OneReceiver()

    do_fn_invoker_args = [
        DoFnSignature(transform.fn),
        _OutputHandler(
            window_fn=window_fn,
            main_receivers=tagged_receivers[None],
            tagged_receivers=tagged_receivers,
            per_element_output_counter=None,
            output_batch_converter=None,
            process_yields_batches=False,
            process_batch_yields_elements=False),
    ]
    do_fn_invoker_kwargs = dict(
        context=DoFnContext(transform.label, state=None),
        side_inputs=side_inputs,
        input_args=args,
        input_kwargs=kwargs,
        user_state_context=None,
        bundle_finalizer_param=DoFn.BundleFinalizerParam(),
    )

    return input_bag.map(get_windowed_value, window_fn).map_partitions(
        apply_dofn_to_bundle,
        do_fn_invoker_args,
        do_fn_invoker_kwargs,
        tagged_receivers,
    )


class GroupByKey(DaskBagOp):
  """Group a PCollection into a mapping of keys to elements."""
  def apply(self, input_bag: db.Bag, side_inputs: OpSide = None) -> db.Bag:
    def key(item):
      return item[0]

    def value(item):
      k, v = item
      return k, [defenestrate(elm[1]) for elm in v]

    return input_bag.groupby(key).map(value)


class Flatten(DaskBagOp):
  """Produces a flattened bag from a collection of bags."""
  def apply(
      self, input_bag: t.List[db.Bag], side_inputs: OpSide = None) -> db.Bag:
    assert isinstance(input_bag, list), 'Must take a sequence of bags!'
    return db.concat(input_bag)


TRANSLATIONS = {
    _Create: Create,
    apache_beam.ParDo: ParDo,
    _GroupByKeyOnly: GroupByKey,
    _Flatten: Flatten,
}
