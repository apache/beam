import datetime
import logging
import random
import time

from typing import TypeVar

from apache_beam import WindowInto
from apache_beam import Windowing
from apache_beam import typehints
from apache_beam.transforms import DoFn
from apache_beam.transforms import Map
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import PTransform
from apache_beam.transforms import ParDo
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import Always
from apache_beam.transforms.window import GlobalWindows
from apache_beam.typehints import Tuple

from apache_beam.io.gcp.datastore.v1new import util
from apache_beam.utils.retry import FuzzedExponentialIntervals

K = TypeVar('K')
T = TypeVar('T')

_LOG = logging.getLogger(__name__)


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class RampupThrottlerTransform(PTransform):
  """A ``PTransform`` that throttles ramp-up to an exponential function.

  An implementation of a client-side throttler that enforces a gradual ramp-up, broadly in line
 * with Datastore best practices. See also https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
  """

  def __init__(self, num_shards):
    """Initializes a ramp-up throttler transform.

     Args:
       num_shards: Number of shards during throttling step
     """
    super(RampupThrottlerTransform, self).__init__()
    self._num_shards = num_shards

  def expand(self, pcoll):
    original_window = pcoll.windowing.windowfn
    throttler_window = Windowing(
      GlobalWindows(),
      triggerfn=Always(),
      accumulation_mode=AccumulationMode.DISCARDING)
    return (pcoll
            | 'Assign random shard keys' >>
            Map(lambda e: (random.randrange(self._num_shards), e))
            .with_input_types(T).with_output_types(Tuple[K, T])
            | 'Prepare window for sharding' >> WindowInto(throttler_window)
            | 'Throttler resharding' >> GroupByKey()
            | 'Throttle for ramp-up' >> ParDo(_RampupThrottlingFn())
            | 'Reset window' >> WindowInto(original_window))


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class _RampupThrottlingFn(DoFn):
  """DoFn that delays output to enforce gradual ramp-up."""

  _BASE_BUDGET = 500
  _RAMP_UP_INTERVAL = datetime.timedelta(minutes=5)

  def to_runner_api_parameter(self, unused_context):
    pass

  def __init__(self, *unused_args, **unused_kwargs):
    super(_RampupThrottlingFn, self).__init__(*unused_args, **unused_kwargs)
    self._successful_ops = util.MovingSum(window_ms=1000, bucket_ms=1000)

  def setup(self):
    print('setup')

  def _calc_max_ops_budget(
    self, first_instant: datetime.datetime, current_instant: datetime.datetime):
    """Function that returns per-second budget according to best practices.

    The exact function is `500 / num_shards * 1.5^max(0, (x-5)/5)`, where x is
    the number of minutes since start time.
    """
    timedelta_since_first = current_instant - first_instant
    growth = max(0.0,
                 timedelta_since_first / _RampupThrottlingFn._RAMP_UP_INTERVAL)
    max_ops_budget = int(
      _RampupThrottlingFn._BASE_BUDGET / self._num_shards * (1.5 ** growth))
    return max_ops_budget

  def process(self, keyed_elem, **kwargs):
    shard_id, elements = keyed_elem
    backoff = iter(
      FuzzedExponentialIntervals(initial_delay_secs=1, num_retries=10000))

    while elements:
      instant = datetime.datetime.now()
      max_ops_budget = self._calc_max_ops_budget(instant)
      current_op_count = self._successful_ops.sum(instant.time())
      available_ops = max_ops_budget - current_op_count

      if available_ops > 0:
        emitted_ops = 0
        while elements and available_ops > 0:
          e = elements.pop()

          yield e
          emitted_ops += 1
          available_ops -= 1
        self._successful_ops.add(instant.time(), emitted_ops)
      else:
        backoff_ms = next(backoff)
        _LOG.info(f'Delaying by {backoff_ms}ms to conform to gradual ramp-up.')
        time.sleep(backoff_ms)
