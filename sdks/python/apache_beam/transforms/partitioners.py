import uuid
import apache_beam as beam
from apache_beam import pvalue
from typing import Optional, TypeVar
from typing import Tuple
from typing import Any
from typing import Callable

T = TypeVar('T')


class Top(beam.PTransform):
  """
    A PTransform that takes a PCollection and partitions it into two
    PCollections.  The first PCollection contains the largest n elements of the
    input PCollection, and the second PCollection contains the remaining
    elements of the input PCollection.

    Parameters:
        n: The number of elements to take from the input PCollection.
        key: A function that takes an element of the input PCollection and
            returns a value to compare for the purpose of determining the top n
            elements, similar to Python's built-in sorted function.
        reverse: If True, the top n elements will be the n smallest elements of
            the input PCollection.

    Example usage:

        >>> with beam.Pipeline() as p:
        ...     top, remaining = (p
        ...         | beam.Create(list(range(10)))
        ...         | partitioners.Top(3))
        ...     # top will contain [7, 8, 9]
        ...     # remaining will contain [0, 1, 2, 3, 4, 5, 6]

    .. note::

        This transform requires that the top PCollection fit into memory.

    """
  def __init__(
      self, n: int, key: Optional[Callable[[Any], Any]] = None, reverse=False):
    _validate_nonzero_positive_int(n)
    self.n = n
    self.key = key
    self.reverse = reverse

  def expand(self,
             pcoll) -> Tuple[pvalue.PCollection[T], pvalue.PCollection[T]]:
    # **Illustrative Example:**
    # Our goal is to return two pcollections, top and
    # remaining.

    # Suppose you want to take the top element from `[1, 2, 2]`. Since we have
    # identical elements, we need to be able to uniquely identify each one,
    # so we assign a unique ID to each:
    # `inputs_with_ids: [(1, "A"), (2, "B"), (2, "C")]`

    # Then we sample, e.g.
    # ``` sample: [(2, "B")] ```
    # To get our goal `top` pcollection, we just strip the uuids from
    # that sample.

    # Now to get the `top` pcollection, we need to return essentially
    # `inputs_with_ids` but without any of the elements fom the sample. To
    # do this, we create a set from `sample`, getting `sample_ids:
    # [set("B")]`. Now that we have this set, we can create our
    # `remaining_with_ids` pcollection by just filtering out
    # `inputs_with_ids` and checking for each element "Does this element's
    # corresponding ID exist in `sample_ids`?"

    # Finally, we just return `top` and strip the IDs as we no longer
    # need them and the user doesn't care about them.
    wrapped_key = lambda elem: self.key(elem[0]) if self.key else elem[0]
    inputs_with_ids = (pcoll | beam.Map(_add_uuid))
    sample = (
        inputs_with_ids
        | beam.combiners.Top.Of(self.n, key=wrapped_key, reverse=self.reverse))
    sample_ids = (
        sample
        | beam.Map(lambda sample_list: set(ele[1] for ele in sample_list)))

    def elem_is_not_sampled(elem, sampled_set):
      return elem[1] not in sampled_set

    remaining = (
        inputs_with_ids
        | beam.Filter(
            elem_is_not_sampled,
            sampled_set=beam.pvalue.AsSingleton(sample_ids))
        | beam.Map(_strip_uuid))
    sample_as_pcoll = (
        sample
        | beam.FlatMap(lambda x: x)
        | "StripSampleIDs" >> beam.Map(_strip_uuid))
    return sample_as_pcoll, remaining


def _validate_nonzero_positive_int(n: Optional[Any]) -> None:
  if not isinstance(n, int):
    raise ValueError("n must be an int")
  if n <= 0:
    raise ValueError("n must be a positive int")


def _add_uuid(element: T) -> Tuple[T, str]:
  return element, uuid.uuid4().hex


def _strip_uuid(element: Tuple[T, str]) -> T:
  return element[0]
