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

"""Module of utilities for SQL magics.

For internal use only; no backward-compatibility guarantees.
"""

# pytype: skip-file

import logging
from typing import Dict
from typing import NamedTuple

import apache_beam as beam
from apache_beam.runners.interactive import interactive_beam as ib

_LOGGER = logging.getLogger(__name__)


def is_namedtuple(cls: type) -> bool:
  """Determines if a class is built from typing.NamedTuple."""
  return (
      isinstance(cls, type) and issubclass(cls, tuple) and
      hasattr(cls, '_fields') and hasattr(cls, '__annotations__'))


def register_coder_for_schema(schema: NamedTuple) -> None:
  """Registers a RowCoder for the given schema if hasn't.

  Notifies the user of what code has been implicitly executed.
  """
  assert is_namedtuple(schema), (
      'Schema %s is not a typing.NamedTuple.' % schema)
  coder = beam.coders.registry.get_coder(schema)
  if not isinstance(coder, beam.coders.RowCoder):
    _LOGGER.warning(
        'Schema %s has not been registered to use a RowCoder. '
        'Automatically registering it by running: '
        'beam.coders.registry.register_coder(%s, '
        'beam.coders.RowCoder)',
        schema.__name__,
        schema.__name__)
    beam.coders.registry.register_coder(schema, beam.coders.RowCoder)


def find_pcolls(
    sql: str, pcolls: Dict[str,
                           beam.PCollection]) -> Dict[str, beam.PCollection]:
  """Finds all PCollections used in the given sql query.

  It does a simple word by word match and calls ib.collect for each PCollection
  found.
  """
  found = {}
  for word in sql.split():
    if word in pcolls:
      found[word] = pcolls[word]
  if found:
    _LOGGER.info('Found PCollections used in the magic: %s.', found)
    _LOGGER.info('Collecting data...')
    for name, pcoll in found.items():
      try:
        _ = ib.collect(pcoll)
      except (KeyboardInterrupt, SystemExit):
        raise
      except:
        _LOGGER.error(
            'Cannot collect data for PCollection %s. Please make sure the '
            'PCollections queried in the sql "%s" are all from a single '
            'pipeline using an InteractiveRunner. Make sure there is no '
            'ambiguity, for example, same named PCollections from multiple '
            'pipelines or notebook re-executions.',
            name,
            sql)
        raise
  return found


def replace_single_pcoll_token(sql: str, pcoll_name: str) -> str:
  """Replaces the pcoll_name used in the sql with 'PCOLLECTION'.

  For sql query using only a single PCollection, the PCollection needs to be
  referred to as 'PCOLLECTION' instead of its variable/tag name.
  """
  words = sql.split()
  token_locations = []
  i = 0
  for word in words:
    if word.lower() == 'from':
      token_locations.append(i + 1)
      i += 2
      continue
    i += 1
  for token_location in token_locations:
    if token_location < len(words) and words[token_location] == pcoll_name:
      words[token_location] = 'PCOLLECTION'
  return ' '.join(words)


def pformat_namedtuple(schema: NamedTuple) -> str:
  return '{}({})'.format(
      schema.__name__,
      ', '.join([
          '{}: {}'.format(k, v.__name__) for k,
          v in schema.__annotations__.items()
      ]))
