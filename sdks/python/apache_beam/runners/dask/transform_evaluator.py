import typing as t
import abc
import dataclasses

import apache_beam
from apache_beam.pipeline import AppliedPTransform

import dask.bag as db


@dataclasses.dataclass
class DaskOp(abc.ABC):
    application: AppliedPTransform
    side_inputs: t.Sequence[t.Any]

    @abc.abstractmethod
    def apply(self, element):
        pass


class NoOp(DaskOp):
    def apply(self, element):
        return element


class Create(DaskOp):
    def apply(self, element):
        assert element is None, 'Create expects no input!'

        original_transform = t.cast(apache_beam.Create, self.application.transform)
        items = original_transform.values
        return db.from_sequence(items)


class Impulse(DaskOp):
    def apply(self, element):
        raise NotImplementedError()


class ParDo(DaskOp):
    def apply(self, element):
        assert element is not None, 'ParDo must receive input!'
        assert isinstance(element, db.Bag)
        assert self.application is not None
        transform = self.application.transform
        assert isinstance(transform, apache_beam.ParDo)


TRANSLATIONS = {
    apache_beam.Create: Create

}