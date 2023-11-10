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

import apache_beam
import dask.bag as db
from apache_beam.pipeline import AppliedPTransform
from apache_beam.runners.dask.overrides import _Create
from apache_beam.runners.dask.overrides import _Flatten
from apache_beam.runners.dask.overrides import _GroupByKeyOnly

OpInput = t.Union[db.Bag, t.Sequence[db.Bag], None]


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
    return db.from_sequence(
        items,
        partition_size=max(
            1, math.ceil(math.sqrt(len(items)) / math.sqrt(100))))


class ParDo(DaskBagOp):
  def apply(self, input_bag: db.Bag) -> db.Bag:
    transform = t.cast(apache_beam.ParDo, self.transform)
    return input_bag.map(
        transform.fn.process, *transform.args, **transform.kwargs).flatten()


class Map(DaskBagOp):
  def apply(self, input_bag: db.Bag) -> db.Bag:
    transform = t.cast(apache_beam.Map, self.transform)
    return input_bag.map(
        transform.fn.process, *transform.args, **transform.kwargs)


class GroupByKey(DaskBagOp):
  def apply(self, input_bag: db.Bag) -> db.Bag:
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
