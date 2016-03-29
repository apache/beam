# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implementation of UnmergedState, backed by Windmill."""

from __future__ import absolute_import

from abc import ABCMeta
from abc import abstractmethod
import cPickle as pickle
import logging


from google.cloud.dataflow.internal import windmill_pb2
from google.cloud.dataflow.transforms import trigger
from google.cloud.dataflow.worker import windmillio


# Max timestamp value used in Windmill requests.
MAX_TIMESTAMP = 0x7fffffffffffffff


class WindmillUnmergedState(trigger.UnmergedState):
  """UnmergedState implementation, backed by Windmill."""

  def __init__(self, state_internals):
    self.internals = state_internals

  def set_global_state(self, tag, value):
    self.internals.access('_global_', tag).add(value)

  def get_global_state(self, tag, default=None):
    return self.internals.access('_global_', tag).get() or default

  def set_timer(self, window, name, time_domain, timestamp):
    namespace = self._encode_window(window)
    self.internals.add_output_timer(namespace, name, time_domain, timestamp)

  def clear_timer(self, window, name, time_domain):
    namespace = self._encode_window(window)
    self.internals.clear_output_timer(namespace, name, time_domain)

  def get_window(self, timer_id):
    return timer_id

  def _encode_window(self, window):
    # TODO(robertwb): This is only true for merging windows (but we currently
    # consider all windows to be merging and pay the costs).
    assert isinstance(window, int)
    return str(window)

  def add_state(self, window, tag, value):
    namespace = self._encode_window(window)
    self.internals.access(namespace, tag).add(value)

  def get_state(self, window, tag):
    namespace = self._encode_window(window)
    return self.internals.access(namespace, tag).get()

  def clear_state(self, window, tag):
    namespace = self._encode_window(window)
    self.internals.access(namespace, tag).clear()


class WindmillStateInternals(object):
  """Internal interface to access data in Windmill via state tags."""

  def __init__(self, reader):
    self.reader = reader
    self.accessed = {}
    self.output_timers = {}

  def access(self, namespace, state_tag):
    """Returns accessor for given namespace and state tag."""
    # Note: namespace currently is either a numeric string or "_global_", and so
    # cannot contain "/".  If this changes, we need to be careful in our
    # construction of the state_key below.
    state_key = '%s/%s' % (namespace, state_tag.tag)
    if state_key not in self.accessed:
      if isinstance(state_tag, trigger.ListStateTag):
        # List state.
        self.accessed[state_key] = WindmillBagAccessor(self.reader, state_key)
      elif isinstance(state_tag, trigger.ValueStateTag):
        # Value state without combiner.
        self.accessed[state_key] = WindmillValueAccessor(self.reader, state_key)
      elif isinstance(state_tag, trigger.CombiningValueStateTag):
        # Value state with combiner.
        self.accessed[state_key] = WindmillCombiningValueAccessor(
            self.reader, state_key, state_tag.combine_fn)
      elif isinstance(state_tag, trigger.WatermarkHoldStateTag):
        # Watermark hold state.
        self.accessed[state_key] = WindmillWatermarkHoldAccessor(
            self.reader, state_key, state_tag.output_time_fn_impl)
      else:
        raise ValueError('Invalid state tag.')
    return self.accessed[state_key]

  def add_output_timer(self, namespace, name, time_domain, timestamp):
    windmill_ts = windmillio.harness_to_windmill_timestamp(timestamp)
    # Note: The character "|" must not be in the given namespace or name
    # since we use it as the delimiter in the combined tag string.
    assert '|' not in namespace
    assert '|' not in name
    self.output_timers[(namespace, name, time_domain)] = windmill_pb2.Timer(
        tag='%s|%s|%s' % (namespace, name, time_domain),
        timestamp=windmill_ts,
        type=time_domain,
        state_family='')

  def clear_output_timer(self, namespace, name, time_domain):
    self.output_timers[(namespace, name, time_domain)] = windmill_pb2.Timer(
        tag='%s|%s|%s' % (namespace, name, time_domain),
        type=time_domain,
        state_family='')

  def persist_to(self, commit_request):
    for unused_key, accessor in self.accessed.iteritems():
      accessor.persist_to(commit_request)
    commit_request.output_timers.extend(self.output_timers.values())


class WindmillStateReader(object):
  """Reader of raw state from Windmill."""

  # The size of Windmill list request responses is capped at this size (or at
  # least one list element, if a single such element would exceed this size).
  MAX_LIST_BYTES = 8 << 20  # 8MB

  def __init__(self, computation_id, key, work_token, windmill):
    self.computation_id = computation_id
    self.key = key
    self.work_token = work_token
    self.windmill = windmill

  def fetch_value(self, state_key):
    """Get the value at given state tag."""
    request = windmill_pb2.GetDataRequest()
    computation_request = windmill_pb2.ComputationGetDataRequest(
        computation_id=self.computation_id)
    keyed_request = windmill_pb2.KeyedGetDataRequest(
        key=self.key,
        work_token=self.work_token)
    keyed_request.values_to_fetch.add(
        tag=state_key,
        state_family='')
    computation_request.requests.extend([keyed_request])
    request.requests.extend([computation_request])
    return self.windmill.GetData(request)

  def fetch_list(self, state_key, request_token=None):
    """Get the list at given state tag."""
    request = windmill_pb2.GetDataRequest()
    computation_request = windmill_pb2.ComputationGetDataRequest(
        computation_id=self.computation_id)
    keyed_request = windmill_pb2.KeyedGetDataRequest(
        key=self.key,
        work_token=self.work_token)
    keyed_request.lists_to_fetch.add(
        tag=state_key,
        state_family='',
        end_timestamp=MAX_TIMESTAMP,
        request_token=request_token or '',
        fetch_max_bytes=WindmillStateReader.MAX_LIST_BYTES)
    computation_request.requests.extend([keyed_request])
    request.requests.extend([computation_request])
    return self.windmill.GetData(request)

  def fetch_watermark_hold(self, state_key):
    """Get the watermark hold at given state tag."""
    request = windmill_pb2.GetDataRequest()
    computation_request = windmill_pb2.ComputationGetDataRequest(
        computation_id=self.computation_id)
    keyed_request = windmill_pb2.KeyedGetDataRequest(
        key=self.key,
        work_token=self.work_token)
    keyed_request.watermark_holds_to_fetch.add(
        tag=state_key,
        state_family='')
    computation_request.requests.extend([keyed_request])
    request.requests.extend([computation_request])
    return self.windmill.GetData(request)


# TODO(ccy): investigate use of coders for Windmill state data.
def encode_value(value):
  return pickle.dumps(value)


def decode_value(encoded):
  return pickle.loads(encoded)


class StateAccessor(object):
  """Interface for accessing state bound to a given tag."""
  __metaclass__ = ABCMeta

  @abstractmethod
  def get(self):
    """Get the state at the bound tag.

    Returns:
      the current value (or accumulated value) for a ValueTag; an interable of
      current values for a ListTag.
    """
    pass

  @abstractmethod
  def add(self, value):
    """Add the given value to the state at the bound tag.

    For a ValueTag with a combiner, this adds the given value through the
    combiner's accumulator.  For a ListTag, this inserts the given value at the
    end of the list state.  For a ValueTag without a combiner, this replaces
    the single value stored in the value state.

    Args:
      value: the value to add.
    """
    pass

  @abstractmethod
  def clear(self):
    """Clears the state at the bound tag."""
    pass

  @abstractmethod
  def persist_to(self, commit_request):
    """Writes state changes to the given WorkItemCommitRequest message."""
    pass


class WindmillValueAccessor(StateAccessor):
  """Accessor for value state in Windmill."""

  def __init__(self, reader, state_key):
    self.reader = reader
    self.state_key = state_key

    self.value = None
    self.fetched = False
    self.modified = False
    self.cleared = False

  def get(self):
    if not self.fetched:
      self._fetch()
    return self.value

  def add(self, value):
    self.modified = True
    self.cleared = False
    # Note: we don't do a deep copy of the added value; it is the caller's
    # responsibility to make sure the value doesn't change until the value
    # is committed to Windmill.
    self.value = value

  def clear(self):
    self.modified = True
    self.cleared = True
    self.value = None

  def _fetch(self):
    """Fetch state from Windmill."""
    result = self.reader.fetch_value(self.state_key)
    for wrapper in result.data:
      for item in wrapper.data:
        for value in item.values:
          if value.value.data == '':  # pylint: disable=g-explicit-bool-comparison
            # When uninitialized, Windmill returns the empty string as the
            # initial value.
            self.value = None
          else:
            try:
              self.value = decode_value(value.value.data)
            except Exception:  # pylint: disable=broad-except
              logging.error(
                  'Error: could not decode value for key %r; '
                  'setting to None: %r.',
                  self.state_key, value.value.data)
              self.value = None
    self.fetched = True

  def persist_to(self, commit_request):
    if not self.modified:
      return

    if self.cleared:
      encoded_value = ''
    else:
      encoded_value = encode_value(self.value)

    commit_request.value_updates.add(
        tag=self.state_key,
        state_family='',
        value=windmill_pb2.Value(
            data=encoded_value,
            timestamp=MAX_TIMESTAMP))


class WindmillCombiningValueAccessor(StateAccessor):
  """Accessor for combining value state in Windmill."""

  def __init__(self, reader, state_key, combine_fn):
    self.reader = reader
    self.state_key = state_key
    self.combine_fn = combine_fn

    self.accum = None
    self.fetched = False
    self.modified = False
    self.cleared = False

  def get(self):
    if not self.fetched:
      self._fetch()
    if self.cleared:
      return (
          self.combine_fn.extract_output(self.combine_fn.create_accumulator()))
    return self.combine_fn.extract_output(self.accum)

  def add(self, value):
    # TODO(ccy): once WindmillStateReader supports asynchronous I/O, we won't
    # have to do this synchronously, i.e. we can fire off the fetch here and
    # return, queuing up (possibly eagerly-combined) values to be accumulated
    # for until we have the response.  We also want to do blind writes, combine
    # new values in persist, and combine all values in fetch.
    if not self.fetched:
      self._fetch()
    if self.cleared:
      self.accum = self.combine_fn.create_accumulator()
      self.cleared = False

    self.modified = True
    self.accum = self.combine_fn.add_inputs(self.accum, [value])

  def clear(self):
    self.modified = True
    self.cleared = True

  def _fetch(self):
    """Fetch state from Windmill."""
    result = self.reader.fetch_value(self.state_key)
    for wrapper in result.data:
      for item in wrapper.data:
        for value in item.values:
          if value.value.data == '':  # pylint: disable=g-explicit-bool-comparison
            # When uninitialized, Windmill returns the empty string as the
            # initial value.
            self.accum = self.combine_fn.create_accumulator()
          else:
            try:
              self.accum = decode_value(value.value.data)
            except Exception:  # pylint: disable=broad-except
              logging.error(
                  'Error: could not decode value; resetting accumulator: %r.',
                  value.value.data)
              self.accum = self.combine_fn.create_accumulator()
    self.fetched = True

  def persist_to(self, commit_request):
    if not self.modified:
      return

    if self.cleared:
      encoded_value = ''
    else:
      encoded_value = encode_value(self.accum)
    commit_request.value_updates.add(
        tag=self.state_key,
        state_family='',
        value=windmill_pb2.Value(
            data=encoded_value,
            timestamp=MAX_TIMESTAMP))


class WindmillBagAccessor(StateAccessor):
  """Accessor for list state in Windmill."""

  class WindmillBagIterable(object):

    def __init__(self, accessor):
      self.accessor = accessor

    def __iter__(self):
      return self.accessor._get_iter()  # pylint: disable=protected-access

  def __init__(self, reader, state_key):
    self.reader = reader
    self.state_key = state_key

    self.cleared = False
    self.encoded_new_values = []

  def get(self):
    # Don't directly iterate here; we want to return an iterable object so that
    # the user may restart iteration if desired.
    return WindmillBagAccessor.WindmillBagIterable(self)

  def _get_iter(self):
    if not self.cleared:
      pass
    # Fetch values from Windmill, followed by values added in this sesison.
    for value in self._fetch():
      yield value
    for value in self.encoded_new_values:
      yield decode_value(value)

  def _fetch(self):
    """Fetch state from Windmill."""
    # TODO(ccy): the Java SDK caches the first page and at the start of each
    # page of values, fires off an asynchronous read for the next page.  We
    # should do this too once we have asynchronous Windmill state reading.
    should_fetch_more = True
    next_request_token = None
    while should_fetch_more:
      result = self.reader.fetch_list(self.state_key,
                                      request_token=next_request_token)
      next_request_token = None
      for wrapper in result.data:
        for datum in wrapper.data:
          for item in datum.lists:
            next_request_token = item.continuation_token
            for value in item.values:
              try:
                yield decode_value(value.data)
              except Exception:  # pylint: disable=broad-except
                logging.error('Could not decode value: %r.', value.data)
                yield None
      should_fetch_more = next_request_token != ''  # pylint: disable=g-explicit-bool-comparison

  def add(self, value):
    # Encode the value here to ensure further mutations of the value don't
    # affect the value eventually committed to Windmill.
    self.encoded_new_values.append(encode_value(value))

  def clear(self):
    self.cleared = True
    self.encoded_new_values = []

  def persist_to(self, commit_request):
    if self.cleared:
      commit_request.list_updates.add(
          tag=self.state_key,
          state_family='',
          end_timestamp=MAX_TIMESTAMP)
    if self.encoded_new_values:
      list_updates = commit_request.list_updates.add(
          tag=self.state_key,
          state_family='')
      for encoded_value in self.encoded_new_values:
        list_updates.values.add(data=encoded_value, timestamp=MAX_TIMESTAMP)


class WindmillWatermarkHoldAccessor(StateAccessor):
  """Accessor for watermark hold state in Windmill."""

  def __init__(self, reader, state_key, output_time_fn_impl):
    self.reader = reader
    self.state_key = state_key
    self.output_time_fn_impl = output_time_fn_impl

    self.hold_time = None
    self.fetched = False
    self.modified = False
    self.cleared = False

  def get(self):
    if not self.fetched:
      self._fetch()
    if self.cleared:
      return None
    return self.hold_time

  def add(self, value):
    # TODO(ccy): once WindmillStateReader supports asynchronous I/O, we won't
    # have to do this synchronously, i.e. we can fire off the fetch here and
    # return, queuing up (possibly eagerly-combined) values to be accumulated
    # into the hold time for until we have the response.
    if not self.fetched:
      self._fetch()

    self.cleared = False
    self.modified = True
    if self.hold_time is None:
      self.hold_time = value
    else:
      self.hold_time = self.output_time_fn_impl.combine(self.hold_time, value)

  def clear(self):
    self.modified = True
    self.cleared = True

  def _fetch(self):
    """Fetch state from Windmill."""
    result = self.reader.fetch_watermark_hold(self.state_key)
    for wrapper in result.data:
      for item in wrapper.data:
        for value in item.watermark_holds:
          if (len(value.timestamps) == 1 and
              value.timestamps[0] == MAX_TIMESTAMP):
            # When uninitialized, Windmill returns MAX_TIMESTAMP
            self.hold_time = None
          else:
            for wm_timestamp in value.timestamps:
              timestamp = windmillio.windmill_to_harness_timestamp(
                  wm_timestamp)
              if self.hold_time is None:
                self.hold_time = timestamp
              else:
                self.hold_time = self.output_time_fn_impl.combine(
                    self.hold_time, timestamp)
    self.fetched = True

  def persist_to(self, commit_request):
    # TODO(ccy): Apparently sending reset=True below is expensive for Windmill
    # if we haven't done a read.  We will need to optimize this if we ever do
    # blind writes here.

    if not self.modified:
      return

    if self.cleared:
      value_to_persist = None
    else:
      value_to_persist = [
          windmillio.harness_to_windmill_timestamp(self.hold_time)]
    commit_request.watermark_holds.add(
        tag=self.state_key,
        state_family='',
        timestamps=value_to_persist,
        reset=True)
