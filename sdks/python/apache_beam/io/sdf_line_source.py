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
from __future__ import absolute_import

import os

from apache_beam import Create
from apache_beam import pvalue
from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange

class SdfReadLineRestrictionProvider(core.RestrictionProvider):
  """ A `core.RestrictionProvider` implementation which provides
  `OffsetRestrictionTracker`."""
  def _get_file_size(self, file_name):
    with open(file_name, 'rb') as f:
      f.seek(0, os.SEEK_END)
      return f.tell()

  def initial_restriction(self, element):
    return OffsetRange(0, self._get_file_size(element['file_pattern']))

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def split(self, element, restriction):
    return restriction.split(element['desired_chunk_size'])

  def restriction_size(self, element, restriction):
    return restriction.size()


class SdfReadLine(core.DoFn):
  """ An SplittableDoFn which reads lines from file and as output."""
  def process(
      self,
      element,
      restriction_tracker=core.DoFn.RestrictionParam(
          SdfReadLineRestrictionProvider())):
    with open(element['file_pattern'], 'rb') as f:
      start = restriction_tracker.start_position()
      f.seek(start)
      if start > 0:
        f.seek(-1, os.SEEK_CUR)
        start -= 1
        start += len(f.readline())
      current = start
      line = f.readline()
      while restriction_tracker.try_claim(current):
        if not line:
          return
        yield line.rstrip(b'\n')
        current += len(line)
        line = f.readline()


class SdfReadLineSource(ptransform.PTransform):
  """ A `ptransform.PTransform` that expands to `SdfReadLine`."""
  MIN_BUNDLE_SIZE = 10

  def __init__(self, file_patterns, desired_chunk_size=None):
    bundle_size = (self.MIN_BUNDLE_SIZE if desired_chunk_size is None
                   else desired_chunk_size)
    self._metadata_elements = [{'file_pattern': file_pattern,
                                'desired_chunk_size': bundle_size}
                               for file_pattern in file_patterns]

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    return (pbegin
            | Create(self._metadata_elements)
            | core.ParDo(SdfReadLine()))
