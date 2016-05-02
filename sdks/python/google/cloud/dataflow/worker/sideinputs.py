# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for handling side inputs."""

import collections



def get_iterator_fn_for_sources(sources):
  """Returns callable that returns iterator over elements for given sources."""
  def _inner():
    for source in sources:
      with source.reader() as reader:
        for value in reader:
          yield value
  return _inner


class EmulatedIterable(collections.Iterable):
  """Emulates an iterable for a side input."""

  def __init__(self, iterator_fn):
    self.iterator_fn = iterator_fn

  def __iter__(self):
    return self.iterator_fn()
