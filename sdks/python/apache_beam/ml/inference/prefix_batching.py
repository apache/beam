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

"""Prefix-aware batching for RunInference model handlers that front an LLM
server with a prefix (KV) cache, such as vLLM.

``ModelHandler(batch_key_fn=...)`` makes ``BatchElements`` route elements that
share a key to the same worker via ``GroupIntoBatches``, so each batch the
server sees is homogeneous and its prefix cache gets hit instead of thrashed.
This module packages the knobs for that into one :class:`PrefixBatchingConfig`
plus the default key extractors a handler installs when the user does not
supply their own:

* :class:`PromptPrefixKeyFn` -- for plain-text prompts (completions API).
* :class:`ChatPrefixKeyFn` -- for chat message lists (chat API).

Both see the element *after* any ``with_preprocess_fn`` has run, i.e. the
prompt / message list the handler will actually send. Nothing here is specific
to a particular server; a handler only decides which extractor is its default.
"""

# pytype: skip-file

import dataclasses
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Union

__all__ = [
    'ChatPrefixKeyFn',
    'PrefixBatchingConfig',
    'PromptPrefixKeyFn',
]

# Characters of prompt (or leading message content) the default extractors key
# on. Long enough to cover a typical system prompt, short enough that keys are
# cheap to hash and shuffle.
DEFAULT_PREFIX_CHAR_LEN = 128
# GroupIntoBatches buffering applied when the handler's own
# ``max_batch_duration_secs`` is unset, so low-traffic keys still flush in
# streaming instead of waiting for a full batch.
DEFAULT_KEYED_BATCH_DURATION_SECS = 1.0

# Accepted forms of ``num_shards``: one count for every key, a per-key table,
# or a callable computed per key (see ``WithBatchKey`` in transforms/util.py).
NumShardsSpec = Union[int, Mapping[Any, int], Callable[[Any], int]]


class PromptPrefixKeyFn:
  """Default prefix key extractor for plain-text prompts.

  Keys a prompt string by its leading ``prefix_char_len`` characters, or by
  everything before the first ``\\n\\n`` when that comes sooner, so prompts
  sharing the same system prompt or instruction header are routed to the same
  worker and batched together.
  """
  def __init__(self, prefix_char_len: int = DEFAULT_PREFIX_CHAR_LEN):
    if prefix_char_len <= 0:
      raise ValueError('prefix_char_len must be positive.')
    self._prefix_char_len = prefix_char_len

  def __call__(self, element: str) -> str:
    if not isinstance(element, str):
      raise TypeError(
          f'Expected str prompt for prefix key extraction, got '
          f'{type(element).__name__}.')
    sep_idx = element.find('\n\n')
    if 0 < sep_idx <= self._prefix_char_len:
      return element[:sep_idx]
    return element[:self._prefix_char_len]


class ChatPrefixKeyFn:
  """Default prefix key extractor for chat message lists.

  Keys a conversation by the role and leading ``prefix_char_len`` characters of
  its first (typically system) message, so conversations sharing the same
  system prompt are routed to the same worker and batched together. Messages
  may be objects with ``role``/``content`` attributes or dicts with those keys.
  """
  def __init__(self, prefix_char_len: int = DEFAULT_PREFIX_CHAR_LEN):
    if prefix_char_len <= 0:
      raise ValueError('prefix_char_len must be positive.')
    self._prefix_char_len = prefix_char_len

  def __call__(self, element: Sequence[Any]) -> str:
    if not isinstance(element, (list, tuple)):
      raise TypeError(
          f'Expected a sequence of chat messages for prefix key extraction, '
          f'got {type(element).__name__}.')
    if not element:
      return ''
    first_msg = element[0]
    role = getattr(first_msg, 'role', None)
    content = getattr(first_msg, 'content', None)
    if role is None and isinstance(first_msg, dict):
      role = first_msg.get('role', '')
      content = first_msg.get('content', '')
    content_str = str(content or '')
    return f'{role or ""}:{content_str[:self._prefix_char_len]}'


@dataclass(frozen=True)
class PrefixBatchingConfig:
  """User-facing knobs for prefix-aware batching.

  Pass an instance to a model handler (e.g.
  ``VLLMCompletionsModelHandler(..., prefix_batching_config=...)``) to turn
  keyed batching on; leave it unset to keep the default unkeyed batching. Each
  field:

  * ``key_fn`` -- callable mapping an element (as it enters batching, i.e.
    after any ``with_preprocess_fn``) to a grouping key. Leave unset (the
    default) to use the handler's default prefix extractor
    (:class:`PromptPrefixKeyFn` or :class:`ChatPrefixKeyFn`).
  * ``prefix_char_len`` -- how many leading characters the default extractor
    keys on (default 128). Ignored when ``key_fn`` is set.
  * ``num_shards`` -- bounds hot-key fanout: an ``int`` applied to every key,
    a ``Mapping[key, int]`` with per-key shard counts for a known skewed
    prefix distribution (e.g. ``{'<the common system prompt>': 8}``; unlisted
    keys get 1), or a ``Callable[[key], int]``. Each key is spread across at
    most that many workers (default 1).

  The handler calls :meth:`resolve` with its default extractor and then
  :meth:`batching_kwargs` to obtain the ``ModelHandler`` arguments.
  """
  key_fn: Optional[Callable[[Any], Any]] = None
  prefix_char_len: int = DEFAULT_PREFIX_CHAR_LEN
  num_shards: Optional[NumShardsSpec] = None

  def resolve(
      self, default_key_fn: Callable[[int], Callable[[Any], Any]]
  ) -> 'PrefixBatchingConfig':
    """Returns a copy whose ``key_fn`` is set.

    Args:
      default_key_fn: Factory (e.g. :class:`PromptPrefixKeyFn`) invoked with
        ``prefix_char_len`` when ``key_fn`` is unset.
    """
    if self.key_fn is not None:
      return self
    return dataclasses.replace(
        self, key_fn=default_key_fn(self.prefix_char_len))

  def batching_kwargs(self,
                      max_batch_duration_secs: Optional[float] = None
                      ) -> dict[str, Any]:
    """Returns the ``ModelHandler`` batching kwargs for a resolved config.

    Args:
      max_batch_duration_secs: The handler's own buffering duration; when
        unset, ``DEFAULT_KEYED_BATCH_DURATION_SECS`` is used so low-traffic
        keys still flush in streaming.
    """
    if self.key_fn is None:
      raise ValueError(
          'PrefixBatchingConfig.key_fn is unset; call resolve(default_key_fn) '
          'first.')
    return {
        'batch_key_fn': self.key_fn,
        'batch_key_num_shards': self.num_shards,
        'max_batch_duration_secs': (
            DEFAULT_KEYED_BATCH_DURATION_SECS
            if max_batch_duration_secs is None else max_batch_duration_secs),
    }


def resolve_batching_kwargs(
    config: Optional[PrefixBatchingConfig],
    default_key_fn: Callable[[int], Callable[[Any], Any]],
    max_batch_duration_secs: Optional[float] = None,
) -> dict[str, Any]:
  """Batching kwargs for an optional config; unkeyed when ``config`` is None.

  Convenience for handlers: always yields ``max_batch_duration_secs`` and, when
  prefix batching is on, ``batch_key_fn`` / ``batch_key_num_shards`` too, so
  the result can be splatted straight into ``ModelHandler.__init__``.
  """
  if config is None:
    return {'max_batch_duration_secs': max_batch_duration_secs}
  return config.resolve(default_key_fn).batching_kwargs(max_batch_duration_secs)
