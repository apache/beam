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
"""

TODO(alxr): Translate ops from https://docs.dask.org/en/latest/bag-api.html.
"""
import typing as t
import abc
import dataclasses

import apache_beam
from apache_beam.pipeline import AppliedPTransform

import dask.bag as db

from apache_beam.runners.dask.overrides import _Create, _GroupByKeyOnly


@dataclasses.dataclass
class DaskBagOp(abc.ABC):
    applied: AppliedPTransform

    @property
    def side_inputs(self):
        return self.applied.side_inputs

    @abc.abstractmethod
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        pass


class NoOp(DaskBagOp):
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        return input_bag


class Create(DaskBagOp):
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        assert input_bag is None, 'Create expects no input!'
        original_transform = t.cast(_Create, self.applied.transform)
        items = original_transform.values
        return db.from_sequence(items)


class ParDo(DaskBagOp):
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        fn = t.cast(apache_beam.ParDo, self.applied.transform).fn
        return input_bag.map(fn).flatten()


class Map(DaskBagOp):
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        fn = t.cast(apache_beam.Map, self.applied.transform).fn
        return input_bag.map(fn)


class GroupByKey(DaskBagOp):
    def apply(self, input_bag: t.Optional[db.Bag]) -> db.Bag:
        def key(item):
            return item[0]

        return input_bag.groupby(key)


TRANSLATIONS = {
    _Create: Create,
    apache_beam.ParDo: ParDo,
    apache_beam.Map: Map,
    _GroupByKeyOnly: GroupByKey,
}
