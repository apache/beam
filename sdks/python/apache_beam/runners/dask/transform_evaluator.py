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
import typing as t
import abc
import dataclasses

import apache_beam
from apache_beam.pipeline import AppliedPTransform

import dask.bag as db

from apache_beam.runners.dask.overrides import _Create


@dataclasses.dataclass
class DaskBagOp(abc.ABC):
    application: AppliedPTransform

    @property
    def side_inputs(self):
        return self.application.side_inputs

    @abc.abstractmethod
    def apply(self, element: db.Bag) -> db.Bag:
        pass


class NoOp(DaskBagOp):
    def apply(self, element: db.Bag) -> db.Bag:
        return element


class Create(DaskBagOp):
    def apply(self, element: db.Bag) -> db.Bag:
        assert element is None, 'Create expects no input!'

        original_transform = t.cast(apache_beam.Create, self.application.transform)
        items = original_transform.values
        return db.from_sequence(items)


class Impulse(DaskBagOp):
    def apply(self, element: db.Bag) -> db.Bag:
        raise NotImplementedError()


class ParDo(DaskBagOp):
    def apply(self, element: db.Bag) -> db.Bag:
        assert element is not None, 'ParDo must receive input!'
        assert isinstance(element, db.Bag)
        assert self.application is not None
        transform = self.application.transform
        assert isinstance(transform, apache_beam.ParDo)

        return element


TRANSLATIONS = {
    _Create: Create
}
