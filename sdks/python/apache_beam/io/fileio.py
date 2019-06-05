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

"""``PTransforms`` for manipulating files in Apache Beam.

Provides reading ``PTransform``\\s, ``MatchFiles``,
``MatchAll``, that produces a ``PCollection`` of records representing a file
and its metadata; and ``ReadMatches``, which takes in a ``PCollection`` of file
metadata records, and produces a ``PCollection`` of ``ReadableFile`` objects.
These transforms currently do not support splitting by themselves.

No backward compatibility guarantees. Everything in this module is experimental.
"""

from __future__ import absolute_import

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io.filesystem import BeamIOError
from apache_beam.utils.annotations import experimental

__all__ = ['EmptyMatchTreatment',
           'MatchFiles',
           'MatchAll',
           'ReadableFile',
           'ReadMatches']


class EmptyMatchTreatment(object):
  """How to treat empty matches in ``MatchAll`` and ``MatchFiles`` transforms.

  If empty matches are disallowed, an error will be thrown if a pattern does not
  match any files."""

  ALLOW = 'ALLOW'
  DISALLOW = 'DISALLOW'
  ALLOW_IF_WILDCARD = 'ALLOW_IF_WILDCARD'

  @staticmethod
  def allow_empty_match(pattern, setting):
    if setting == EmptyMatchTreatment.ALLOW:
      return True
    elif setting == EmptyMatchTreatment.ALLOW_IF_WILDCARD and '*' in pattern:
      return True
    elif setting == EmptyMatchTreatment.DISALLOW:
      return False
    else:
      raise ValueError(setting)


class _MatchAllFn(beam.DoFn):

  def __init__(self, empty_match_treatment):
    self._empty_match_treatment = empty_match_treatment

  def process(self, file_pattern):
    # TODO: Should we batch the lookups?
    match_results = filesystems.FileSystems.match([file_pattern])
    match_result = match_results[0]

    if (not match_result.metadata_list
        and not EmptyMatchTreatment.allow_empty_match(
            file_pattern, self._empty_match_treatment)):
      raise BeamIOError(
          'Empty match for pattern %s. Disallowed.' % file_pattern)

    return match_result.metadata_list


@experimental()
class MatchFiles(beam.PTransform):
  """Matches a file pattern using ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""

  def __init__(self,
               file_pattern,
               empty_match_treatment=EmptyMatchTreatment.ALLOW_IF_WILDCARD):
    self._file_pattern = file_pattern
    self._empty_match_treatment = empty_match_treatment

  def expand(self, pcoll):
    return (pcoll.pipeline
            | beam.Create([self._file_pattern])
            | MatchAll())


@experimental()
class MatchAll(beam.PTransform):
  """Matches file patterns from the input PCollection via ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""

  def __init__(self, empty_match_treatment=EmptyMatchTreatment.ALLOW):
    self._empty_match_treatment = empty_match_treatment

  def expand(self, pcoll):
    return (pcoll
            | beam.ParDo(_MatchAllFn(self._empty_match_treatment)))


class _ReadMatchesFn(beam.DoFn):

  def __init__(self, compression, skip_directories):
    self._compression = compression
    self._skip_directories = skip_directories

  def process(self, file_metadata):
    metadata = (filesystem.FileMetadata(file_metadata, 0)
                if isinstance(file_metadata, (str, unicode))
                else file_metadata)

    if metadata.path.endswith('/') and self._skip_directories:
      return
    elif metadata.path.endswith('/'):
      raise BeamIOError(
          'Directories are not allowed in ReadMatches transform.'
          'Found %s.' % metadata.path)

    # TODO: Mime type? Other arguments? Maybe arguments passed in to transform?
    yield ReadableFile(metadata)


class ReadableFile(object):
  """A utility class for accessing files."""

  def __init__(self, metadata):
    self.metadata = metadata

  def open(self, mime_type='text/plain'):
    return filesystems.FileSystems.open(self.metadata.path)

  def read(self):
    return self.open().read()

  def read_utf8(self):
    return self.open().read().decode('utf-8')


@experimental()
class ReadMatches(beam.PTransform):
  """Converts each result of MatchFiles() or MatchAll() to a ReadableFile.

   This helps read in a file's contents or obtain a file descriptor."""

  def __init__(self, compression=None, skip_directories=True):
    self._compression = compression
    self._skip_directories = skip_directories

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_ReadMatchesFn(self._compression,
                                             self._skip_directories))
