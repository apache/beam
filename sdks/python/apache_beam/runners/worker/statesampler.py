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

# pytype: skip-file

import contextlib
import threading
from typing import TYPE_CHECKING
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Union

from apache_beam.runners import common
from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName
from apache_beam.utils.threads import ParentAwareThread

try:
  from apache_beam.runners.worker import statesampler_fast as statesampler_impl  # type: ignore
  FAST_SAMPLER = True
except ImportError:
  from apache_beam.runners.worker import statesampler_slow as statesampler_impl
  FAST_SAMPLER = False

if TYPE_CHECKING:
  from apache_beam.metrics.execution import MetricsContainer

# Global dictionary to store state samplers keyed by thread id.
_STATE_SAMPLERS = {}
_STATE_SAMPLERS_LOCK = threading.Lock()


def set_current_tracker(tracker):
  """Sets state tracker for the calling thread."""
  with _STATE_SAMPLERS_LOCK:
    if (tracker is None):
      _STATE_SAMPLERS.pop(threading.get_ident())
      return

    _STATE_SAMPLERS[threading.get_ident()] = tracker


def get_current_tracker():
  """Retrieve state tracker for the calling thread.

  If the thread is a ParentAwareThread (child thread that work was handed off
  to) it attempts to retrieve the tracker associated with its parent thread.

  """
  current_thread_id = threading.get_ident()

  with _STATE_SAMPLERS_LOCK:
    if current_thread_id in _STATE_SAMPLERS:
      return _STATE_SAMPLERS[current_thread_id]

    current_thread = threading.current_thread()
    if isinstance(current_thread, ParentAwareThread
                  ) and current_thread.parent_thread_id in _STATE_SAMPLERS:
      return _STATE_SAMPLERS.get(current_thread.parent_thread_id)
  return None


_INSTRUCTION_IDS = threading.local()


def get_current_instruction_id():
  try:
    return _INSTRUCTION_IDS.instruction_id
  except AttributeError:
    return None


@contextlib.contextmanager
def instruction_id(id):
  try:
    _INSTRUCTION_IDS.instruction_id = id
    yield
  finally:
    _INSTRUCTION_IDS.instruction_id = None


def for_test():
  set_current_tracker(StateSampler('test', CounterFactory()))
  return get_current_tracker()


StateSamplerInfo = NamedTuple(
    'StateSamplerInfo',
    [('state_name', CounterName), ('transition_count', int),
     ('time_since_transition', int),
     ('tracked_thread', Optional[threading.Thread])])

# Default period for sampling current state of pipeline execution.
DEFAULT_SAMPLING_PERIOD_MS = 200


class StateSampler(statesampler_impl.StateSampler):
  def __init__(
      self,
      prefix: str,
      counter_factory,
      sampling_period_ms=DEFAULT_SAMPLING_PERIOD_MS):
    self._prefix = prefix
    self._counter_factory = counter_factory
    self._states_by_name: Dict[CounterName, statesampler_impl.ScopedState] = {}
    self.sampling_period_ms = sampling_period_ms
    self.tracked_thread: Optional[threading.Thread] = None
    self.finished = False
    self.started = False
    super().__init__(sampling_period_ms)

  @property
  def stage_name(self) -> str:
    return self._prefix

  def stop(self) -> None:
    set_current_tracker(None)
    super().stop()

  def stop_if_still_running(self) -> None:
    if self.started and not self.finished:
      self.stop()

  def start(self) -> None:
    self.tracked_thread = threading.current_thread()
    set_current_tracker(self)
    super().start()
    self.started = True

  def get_info(self) -> StateSamplerInfo:
    """Returns StateSamplerInfo with transition statistics."""
    return StateSamplerInfo(
        self.current_state().name,
        self.state_transition_count,
        self.time_since_transition,
        self.tracked_thread)

  def scoped_state(
      self,
      name_context: Union[str, 'common.NameContext'],
      state_name: str,
      io_target=None,
      metrics_container: Optional['MetricsContainer'] = None
  ) -> statesampler_impl.ScopedState:
    """Returns a ScopedState object associated to a Step and a State.

    Args:
      name_context: common.NameContext. It is the step name information.
      state_name: str. It is the state name (e.g. process / start / finish).
      io_target:
      metrics_container: MetricsContainer. The step's metrics container.

    Returns:
      A ScopedState that keeps the execution context and is able to switch it
      for the execution thread.
    """
    if not isinstance(name_context, common.NameContext):
      name_context = common.NameContext(name_context)

    counter_name = CounterName(
        state_name + '-msecs',
        stage_name=self._prefix,
        step_name=name_context.metrics_name(),
        io_target=io_target)
    if counter_name in self._states_by_name:
      return self._states_by_name[counter_name]
    else:
      output_counter = self._counter_factory.get_counter(
          counter_name, Counter.SUM)
      self._states_by_name[counter_name] = super()._scoped_state(
          counter_name, name_context, output_counter, metrics_container)
      return self._states_by_name[counter_name]

  def commit_counters(self) -> None:
    """Updates output counters with latest state statistics."""
    for state in self._states_by_name.values():
      state_msecs = int(1e-6 * state.nsecs)
      state.counter.update(state_msecs - state.counter.value())
