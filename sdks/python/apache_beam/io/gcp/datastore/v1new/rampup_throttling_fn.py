import datetime
import logging
import time

from typing import TypeVar

from apache_beam import typehints
from apache_beam.transforms import DoFn

from apache_beam.io.gcp.datastore.v1new import util
from apache_beam.utils.retry import FuzzedExponentialIntervals

T = TypeVar('T')

_LOG = logging.getLogger(__name__)


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class RampupThrottlingFn(DoFn):
  """A ``DoFn`` that throttles ramp-up following an exponential function.

  An implementation of a client-side throttler that enforces a gradual ramp-up,
  broadly in line with Datastore best practices. See also
  https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic.
  """

  def to_runner_api_parameter(self, unused_context):
    from apache_beam.internal import pickler
    config = {
        'num_workers': self._num_workers,
    }
    return 'beam:fn:rampup_throttling:v0', pickler.dumps(config)

  _BASE_BUDGET = 500
  _RAMP_UP_INTERVAL = datetime.timedelta(minutes=5)

  def __init__(self, num_workers, *unused_args, **unused_kwargs):
    """Initializes a ramp-up throttler transform.

     Args:
       num_workers: A hint for the expected number of workers, used to derive
                    the local rate limit.
     """
    super(RampupThrottlingFn, self).__init__(*unused_args, **unused_kwargs)
    self._num_workers = num_workers
    self._successful_ops = util.MovingSum(window_ms=1000, bucket_ms=1000)
    self._first_instant = datetime.datetime.now()

  def _calc_max_ops_budget(self, first_instant: datetime.datetime,
      current_instant: datetime.datetime):
    """Function that returns per-second budget according to best practices.

    The exact function is `500 / num_shards * 1.5^max(0, (x-5)/5)`, where x is
    the number of minutes since start time.
    """
    timedelta_since_first = current_instant - first_instant
    growth = max(0.0, ((timedelta_since_first - self._RAMP_UP_INTERVAL)
                       / self._RAMP_UP_INTERVAL))
    max_ops_budget = int(
        self._BASE_BUDGET / self._num_workers * (1.5 ** growth))
    return max_ops_budget

  def process(self, element, **kwargs):
    backoff = iter(
        FuzzedExponentialIntervals(initial_delay_secs=1, num_retries=10000))

    while True:
      instant = datetime.datetime.now()
      max_ops_budget = self._calc_max_ops_budget(self._first_instant, instant)
      current_op_count = self._successful_ops.sum(instant.timestamp() * 1000)
      available_ops = max_ops_budget - current_op_count

      if available_ops > 0:
        self._successful_ops.add(instant.timestamp() * 1000, 1)
        return element
      else:
        backoff_ms = next(backoff)
        _LOG.info(f'Delaying by {backoff_ms}ms to conform to gradual ramp-up.')
        time.sleep(backoff_ms)
