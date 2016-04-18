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
"""Windmill sources and sinks.

Windmill sources and sinks are used internally in streaming pipelines.
"""

from __future__ import absolute_import

from google.cloud.dataflow.coders import observable
from google.cloud.dataflow.io import coders
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.io import pubsub
from google.cloud.dataflow.transforms.timeutil import TimeDomain
from google.cloud.dataflow.transforms.timeutil import Timestamp
from google.cloud.dataflow.transforms.window import GlobalWindows
from google.cloud.dataflow.transforms.window import WindowedValue


def harness_to_windmill_timestamp(timestamp):
  # The timestamp taken by Windmill is in microseconds.
  return timestamp.micros


def windmill_to_harness_timestamp(windmill_timestamp):
  # The timestamp given by Windmill is in microseconds.
  return Timestamp(micros=windmill_timestamp)


class PubSubWindmillSource(pubsub.PubSubSource):
  """Internal worker PubSubSource which reads from Windmill."""

  def __init__(self, context, topic, subscription, coder):
    super(PubSubWindmillSource, self).__init__(topic, subscription, coder)
    self.context = context

  def reader(self):
    return PubSubWindmillReader(self)


class PubSubWindmillReader(iobase.NativeSourceReader):
  """Internal worker Windmill PubSub reader."""

  def __init__(self, source):
    self.source = source

  def __iter__(self):
    for bundle in self.source.context.work_item.message_bundles:
      for message in bundle.messages:
        yield GlobalWindows.WindowedValue(
            self.source.coder.decode(message.data),
            timestamp=windmill_to_harness_timestamp(message.timestamp))

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  @property
  def returns_windowed_values(self):
    """Returns whether this reader returns windowed values."""
    return True


class PubSubWindmillSink(pubsub.PubSubSink):
  """Internal worker PubSubSink which writes to Windmill."""

  def __init__(self, context, coder, topic, timestamp_label, id_label):
    super(PubSubWindmillSink, self).__init__(topic, coder=coder)
    self.context = context
    self.timestamp_label = timestamp_label
    self.id_label = id_label

  def writer(self):
    return PubSubWindmillWriter(self)


class PubSubWindmillWriter(iobase.NativeSinkWriter):
  """Internal worker Windmill PubSub writer."""

  def __init__(self, sink):
    self.sink = sink

    # Avoid dependency on gRPC during testing.
    # pylint: disable=g-import-not-at-top
    from google.cloud.dataflow.internal import windmill_pb2
    # pylint: enable=g-import-not-at-top
    self.windmill_pb2 = windmill_pb2

  def __enter__(self):
    self.output_message_bundle = self.windmill_pb2.PubSubMessageBundle(
        topic=self.sink.topic,
        timestamp_label=self.sink.timestamp_label,
        id_label=self.sink.id_label)
    return self

  @property
  def takes_windowed_values(self):
    """Returns whether this writer takes windowed values."""
    return True

  def Write(self, windowed_value):
    data = self.sink.coder.encode(windowed_value.value)
    timestamp = harness_to_windmill_timestamp(windowed_value.timestamp)
    self.output_message_bundle.messages.add(data=data, timestamp=timestamp)

  def __exit__(self, exception_type, exception_value, traceback):
    if self.output_message_bundle and self.output_message_bundle.messages:
      self.sink.context.workitem_commit_request.pubsub_messages.extend(
          [self.output_message_bundle])
    self.output_message_bundle = None


class WindmillSink(iobase.NativeSink):
  """Sink for writing to a given Cloud Pubsub topic."""

  def __init__(self, context, stream_id, coder):
    self.context = context
    self.stream_id = stream_id
    self.coder = coder

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'windmill'

  def writer(self):
    return WindmillWriter(self)


class WindmillWriter(iobase.NativeSinkWriter):
  """Internal worker Windmill writer."""

  def __init__(self, sink):
    self.sink = sink

    self.key_coder = self.sink.coder.key_coder()
    value_coder = self.sink.coder.value_coder()
    self.wv_coder = coders.WindowedValueCoder(value_coder)

    # Avoid dependency on gRPC during testing.
    # pylint: disable=g-import-not-at-top
    from google.cloud.dataflow.internal import windmill_pb2
    # pylint: enable=g-import-not-at-top
    self.windmill_pb2 = windmill_pb2

  def __enter__(self):
    self.keyed_output = {}
    return self

  @property
  def takes_windowed_values(self):
    """Returns whether this writer takes windowed values."""
    return True

  def Write(self, windowed_kv):
    # WindmillWriter takes windowed values, reifies the windows and writes the
    # resulting windowed value to Windmill.  Note that in this streaming case,
    # the service does not add a ReifyWindows step, so we do that here.
    key, value = windowed_kv.value
    timestamp = windowed_kv.timestamp
    wm_timestamp = harness_to_windmill_timestamp(timestamp)
    windows = windowed_kv.windows
    windowed_value = WindowedValue(value, timestamp, windows)

    encoded_key = self.key_coder.encode(key)
    encoded_value = self.wv_coder.encode(windowed_value)
    # TODO(ccy): In the future, we will populate metadata with PaneInfo
    # details.
    metadata = ''

    # Add to output for key.
    if encoded_key not in self.keyed_output:
      self.keyed_output[encoded_key] = (
          self.windmill_pb2.KeyedMessageBundle(key=encoded_key))
    self.keyed_output[encoded_key].messages.add(
        timestamp=wm_timestamp,
        data=encoded_value,
        metadata=metadata)

  def __exit__(self, exception_type, exception_value, traceback):
    self.sink.context.workitem_commit_request.output_messages.add(
        destination_stream_id=self.sink.stream_id,
        bundles=self.keyed_output.values())
    del self.keyed_output


class WindmillTimer(object):
  """Timer sent by Windmill."""

  def __init__(self, key, namespace, name, time_domain, timestamp,
               state_family):
    self.key = key
    self.namespace = namespace
    self.name = name
    self.time_domain = time_domain
    self.timestamp = timestamp
    self.state_family = state_family

  def __repr__(self):
    return ('WindmillTimer(key=%s, namespace=%s, name=%s, time_domain=%s, '
            'timestamp=%s, state_family=%s)') % (self.key, self.namespace,
                                                 self.name, self.time_domain,
                                                 self.timestamp,
                                                 self.state_family)


class KeyedWorkItem(observable.ObservableMixin):
  """Keyed work item used by a StreamingGroupAlsoByWindowsOperation."""

  def __init__(self, work_item, coder):
    super(KeyedWorkItem, self).__init__()
    self.work_item = work_item
    self.coder = coder
    self.key_coder = coder.key_coder()
    value_coder = coder.value_coder()
    self.wv_coder = coders.WindowedValueCoder(value_coder)
    self.key = self.key_coder.decode(work_item.key)

    # Avoid dependency on gRPC during testing.
    # pylint: disable=g-import-not-at-top
    from google.cloud.dataflow.internal import windmill_pb2
    # pylint: enable=g-import-not-at-top
    self.windmill_pb2 = windmill_pb2

  def elements(self):
    for bundle in self.work_item.message_bundles:
      for message in bundle.messages:
        element = self.wv_coder.decode(message.data)
        self.notify_observers(message.data, is_encoded=True)
        yield element

  def timers(self):
    if self.work_item.timers:
      for timer_item in self.work_item.timers.timers:
        (namespace, name, unused_time_domain) = timer_item.tag.split('|')
        yield WindmillTimer(
            key=self.key,
            namespace=namespace,
            name=name,
            time_domain=TimeDomain.from_string(
                self.windmill_pb2.Timer.Type.Name(timer_item.type)),
            timestamp=windmill_to_harness_timestamp(timer_item.timestamp),
            state_family=timer_item.state_family)

  def __repr__(self):
    return '<%s %s>' % (self.__class__.__name__, self.key)


class WindowingWindmillSource(iobase.NativeSource):
  """Internal worker PubSubSource which reads from Windmill."""

  def __init__(self, context, stream_id, coder):
    self.context = context
    self.coder = coder

  def reader(self):
    return WindowingWindmillReader(self)


class WindowingWindmillReader(iobase.NativeSourceReader):
  """Internal worker Windmill PubSub reader."""

  def __init__(self, source):
    self.source = source

  def __iter__(self):
    return iter([KeyedWorkItem(self.source.context.work_item,
                               self.source.coder)])

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass
