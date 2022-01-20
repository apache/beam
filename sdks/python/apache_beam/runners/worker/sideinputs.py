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

"""Utilities for handling side inputs."""

# pytype: skip-file

import logging
import queue
import threading
import traceback
from collections import abc

from apache_beam.coders import observable
from apache_beam.io import iobase
from apache_beam.runners.worker import opcounters
from apache_beam.transforms import window
from apache_beam.utils.sentinel import Sentinel

# This module is experimental. No backwards-compatibility guarantees.

# Maximum number of reader threads for reading side input sources, per side
# input.
MAX_SOURCE_READER_THREADS = 15

# Number of slots for elements in side input element queue.  Note that this
# value is intentionally smaller than MAX_SOURCE_READER_THREADS so as to reduce
# memory pressure of holding potentially-large elements in memory.  Note that
# the number of pending elements in memory is equal to the sum of
# MAX_SOURCE_READER_THREADS and ELEMENT_QUEUE_SIZE.
ELEMENT_QUEUE_SIZE = 10

# Special element value sentinel for signaling reader state.
READER_THREAD_IS_DONE_SENTINEL = Sentinel.sentinel

# Used to efficiently window the values of non-windowed side inputs.
_globally_windowed = window.GlobalWindows.windowed_value(None).with_value

_LOGGER = logging.getLogger(__name__)


class PrefetchingSourceSetIterable(object):
  """Value iterator that reads concurrently from a set of sources."""
  def __init__(
      self,
      sources,
      max_reader_threads=MAX_SOURCE_READER_THREADS,
      read_counter=None,
      element_counter=None):
    self.sources = sources
    self.num_reader_threads = min(max_reader_threads, len(self.sources))

    # Queue for sources that are to be read.
    self.sources_queue = queue.Queue()
    for source in sources:
      self.sources_queue.put(source)
    # Queue for elements that have been read.
    self.element_queue = queue.Queue(ELEMENT_QUEUE_SIZE)
    # Queue for exceptions encountered in reader threads; to be rethrown.
    self.reader_exceptions = queue.Queue()
    # Whether we have already iterated; this iterable can only be used once.
    self.already_iterated = False
    # Whether an error was encountered in any source reader.
    self.has_errored = False

    self.read_counter = read_counter or opcounters.NoOpTransformIOCounter()
    self.element_counter = element_counter
    self.reader_threads = []
    self._start_reader_threads()

  def add_byte_counter(self, reader):
    """Adds byte counter observer to a side input reader.

    Args:
      reader: A reader that should inherit from ObservableMixin to have
        bytes tracked.
    """
    def update_bytes_read(record_size, is_record_size=False, **kwargs):
      # Let the reader report block size.
      if is_record_size:
        self.read_counter.add_bytes_read(record_size)

    if isinstance(reader, observable.ObservableMixin):
      reader.register_observer(update_bytes_read)

  def _start_reader_threads(self):
    for _ in range(0, self.num_reader_threads):
      t = threading.Thread(target=self._reader_thread)
      t.daemon = True
      t.start()
      self.reader_threads.append(t)

  def _reader_thread(self):
    # pylint: disable=too-many-nested-blocks
    try:
      while True:
        try:
          source = self.sources_queue.get_nowait()
          if isinstance(source, iobase.BoundedSource):
            for value in source.read(source.get_range_tracker(None, None)):
              if self.has_errored:
                # If any reader has errored, just return.
                return
              if isinstance(value, window.WindowedValue):
                self.element_queue.put(value)
              else:
                self.element_queue.put(_globally_windowed(value))
          else:
            # Native dataflow source.
            with source.reader() as reader:
              # The tracking of time spend reading and bytes read from side
              # inputs is kept behind an experiment flag to test performance
              # impact.
              self.add_byte_counter(reader)
              returns_windowed_values = reader.returns_windowed_values
              for value in reader:
                if self.has_errored:
                  # If any reader has errored, just return.
                  return
                if returns_windowed_values:
                  self.element_queue.put(value)
                else:
                  self.element_queue.put(_globally_windowed(value))
        except queue.Empty:
          return
    except Exception as e:  # pylint: disable=broad-except
      _LOGGER.error(
          'Encountered exception in PrefetchingSourceSetIterable '
          'reader thread: %s',
          traceback.format_exc())
      self.reader_exceptions.put(e)
      self.has_errored = True
    finally:
      self.element_queue.put(READER_THREAD_IS_DONE_SENTINEL)

  def __iter__(self):
    # pylint: disable=too-many-nested-blocks
    if self.already_iterated:
      raise RuntimeError(
          'Can only iterate once over PrefetchingSourceSetIterable instance.')
    self.already_iterated = True

    # The invariants during execution are:
    # 1) A worker thread always posts the sentinel as the last thing it does
    #    before exiting.
    # 2) We always wait for all sentinels and then join all threads.
    num_readers_finished = 0
    try:
      while True:
        try:
          with self.read_counter:
            element = self.element_queue.get()
          if element is READER_THREAD_IS_DONE_SENTINEL:
            num_readers_finished += 1
            if num_readers_finished == self.num_reader_threads:
              return
          else:
            if self.element_counter:
              self.element_counter.update_from(element)
              yield element
              self.element_counter.update_collect()
            else:
              yield element
        finally:
          if self.has_errored:
            raise self.reader_exceptions.get()
    except GeneratorExit:
      self.has_errored = True
      raise
    finally:
      while num_readers_finished < self.num_reader_threads:
        element = self.element_queue.get()
        if element is READER_THREAD_IS_DONE_SENTINEL:
          num_readers_finished += 1
      for t in self.reader_threads:
        t.join()


def get_iterator_fn_for_sources(
    sources,
    max_reader_threads=MAX_SOURCE_READER_THREADS,
    read_counter=None,
    element_counter=None):
  """Returns callable that returns iterator over elements for given sources."""
  def _inner():
    return iter(
        PrefetchingSourceSetIterable(
            sources,
            max_reader_threads=max_reader_threads,
            read_counter=read_counter,
            element_counter=element_counter))

  return _inner


class EmulatedIterable(abc.Iterable):
  """Emulates an iterable for a side input."""
  def __init__(self, iterator_fn):
    self.iterator_fn = iterator_fn

  def __iter__(self):
    return self.iterator_fn()
