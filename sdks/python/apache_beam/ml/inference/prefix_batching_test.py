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
import unittest

from apache_beam.ml.inference import prefix_batching
from apache_beam.ml.inference.prefix_batching import ChatPrefixKeyFn
from apache_beam.ml.inference.prefix_batching import PrefixBatchingConfig
from apache_beam.ml.inference.prefix_batching import PromptPrefixKeyFn


class _Msg:
  def __init__(self, role, content):
    self.role = role
    self.content = content


class PrefixBatchingTest(unittest.TestCase):
  def test_prompt_prefix_key_fn(self):
    key_fn = PromptPrefixKeyFn(prefix_char_len=5)
    self.assertEqual(key_fn('abcdefghij'), 'abcde')
    # Stops at the first blank line when it comes before prefix_char_len.
    self.assertEqual(PromptPrefixKeyFn(prefix_char_len=20)('ab\n\ncdef'), 'ab')
    self.assertEqual(PromptPrefixKeyFn(prefix_char_len=1)('ab\n\ncdef'), 'a')
    with self.assertRaises(TypeError):
      key_fn({'prompt': 'abcdefghij'})
    with self.assertRaises(ValueError):
      PromptPrefixKeyFn(prefix_char_len=0)

  def test_prompt_prefix_key_fn_delimiters(self):
    prompt = 'Foo, bar, baz. Choose one of these options:\n- x\n- y'
    self.assertEqual(
        PromptPrefixKeyFn(delimiters=(':', '\n\n'))(prompt),
        'Foo, bar, baz. Choose one of these options')
    # The earliest delimiter wins.
    self.assertEqual(PromptPrefixKeyFn(delimiters=('\n', ':'))('a:b\nc'), 'a')
    # No delimiters: always the leading prefix_char_len characters.
    self.assertEqual(
        PromptPrefixKeyFn(prefix_char_len=4, delimiters=())('ab\n\ncdef'),
        'ab\n\n')
    # Only delimiters starting within prefix_char_len count, and a delimiter
    # at position 0 does not produce an empty key.
    self.assertEqual(
        PromptPrefixKeyFn(prefix_char_len=3, delimiters=(':', ))('abcd:e'),
        'abc')
    self.assertEqual(
        PromptPrefixKeyFn(prefix_char_len=3, delimiters=(':', ))('abc:e'),
        'abc')
    self.assertEqual(
        PromptPrefixKeyFn(prefix_char_len=5, delimiters=(':', ))(':ab:c'),
        ':ab')
    with self.assertRaises(TypeError):
      PromptPrefixKeyFn(delimiters=':')
    with self.assertRaises(ValueError):
      PromptPrefixKeyFn(delimiters=('', ))

  def test_prompt_prefix_key_fn_searches_only_the_prefix(self):
    # A delimiter far past prefix_char_len is never looked for.
    prompt = 'x' * 10 + '\n\n' + 'y' * 10
    self.assertEqual(PromptPrefixKeyFn(prefix_char_len=4)(prompt), 'xxxx')

  def test_chat_prefix_key_fn(self):
    key_fn = ChatPrefixKeyFn(prefix_char_len=3)
    msgs = [_Msg('system', 'You are A'), _Msg('user', 'Hello')]
    self.assertEqual(key_fn(msgs), 'system:You')
    # Dict-style messages and empty conversations are handled too.
    self.assertEqual(
        key_fn([{
            'role': 'system', 'content': 'You are B'
        }]), 'system:You')
    self.assertEqual(key_fn([]), '')
    with self.assertRaises(TypeError):
      key_fn({'messages': msgs})
    # Delimiters apply to the first message's content.
    self.assertEqual(
        ChatPrefixKeyFn(delimiters=(':', ))([_Msg('system', 'Rules: be nice')]),
        'system:Rules')
    with self.assertRaises(ValueError):
      ChatPrefixKeyFn(prefix_char_len=0)

  def test_chat_prefix_key_fn_rejects_unknown_message_schema(self):
    """Unknown schemas must fail loudly instead of all sharing one key."""
    key_fn = ChatPrefixKeyFn()
    with self.assertRaisesRegex(TypeError, 'key_fn'):
      key_fn([{'speaker': 'system', 'text': 'You are A'}])
    with self.assertRaisesRegex(TypeError, 'key_fn'):
      key_fn(['You are A'])
    # A message with a None content (e.g. an assistant tool call) is still a
    # valid chat message.
    self.assertEqual(
        key_fn([{
            'role': 'assistant', 'content': None
        }]), 'assistant:')

  def test_config_resolve_and_batching_kwargs(self):
    # Default: the handler's extractor is built with prefix_char_len.
    cfg = PrefixBatchingConfig(prefix_char_len=32, num_shards=4)
    resolved = cfg.resolve(PromptPrefixKeyFn)
    self.assertIsInstance(resolved.key_fn, PromptPrefixKeyFn)
    self.assertEqual(resolved.key_fn._prefix_char_len, 32)
    self.assertEqual(
        resolved.batching_kwargs(),
        {
            'batch_key_fn': resolved.key_fn,
            'batch_key_num_shards': 4,
            'max_batch_duration_secs': prefix_batching.
            DEFAULT_KEYED_BATCH_DURATION_SECS,
        })
    # An explicit handler duration wins over the keyed default.
    self.assertEqual(
        resolved.batching_kwargs(
            max_batch_duration_secs=2.5)['max_batch_duration_secs'],
        2.5)
    # A custom key_fn is kept verbatim.
    custom = PrefixBatchingConfig(key_fn=len).resolve(PromptPrefixKeyFn)
    self.assertIs(custom.key_fn, len)
    with self.assertRaises(ValueError):
      PrefixBatchingConfig().batching_kwargs()

  def test_config_prefix_delimiters(self):
    # Unset: the extractor's own default.
    self.assertEqual(
        PrefixBatchingConfig().resolve(PromptPrefixKeyFn).key_fn._delimiters,
        prefix_batching.DEFAULT_PROMPT_DELIMITERS)
    self.assertEqual(
        PrefixBatchingConfig().resolve(ChatPrefixKeyFn).key_fn._delimiters, ())
    # Set: passed to whichever default extractor the handler uses.
    for default_key_fn in (PromptPrefixKeyFn, ChatPrefixKeyFn):
      resolved = PrefixBatchingConfig(
          prefix_delimiters=(':', )).resolve(default_key_fn)
      self.assertEqual(resolved.key_fn._delimiters, (':', ))

  def test_resolve_batching_kwargs_without_config(self):
    # No config -> unkeyed batching with the handler's duration passed through.
    self.assertEqual(
        prefix_batching.resolve_batching_kwargs(None, PromptPrefixKeyFn, 3.0),
        {'max_batch_duration_secs': 3.0})
    self.assertEqual(
        prefix_batching.resolve_batching_kwargs(None, PromptPrefixKeyFn),
        {'max_batch_duration_secs': None})
    kwargs = prefix_batching.resolve_batching_kwargs(
        PrefixBatchingConfig(), ChatPrefixKeyFn)
    self.assertIsInstance(kwargs['batch_key_fn'], ChatPrefixKeyFn)
    # Prefix keys are sharded by default so a hot prefix is not served by a
    # single worker.
    self.assertEqual(kwargs['batch_key_num_shards'], 8)
    self.assertEqual(
        kwargs['batch_key_num_shards'], prefix_batching.DEFAULT_NUM_SHARDS)
    self.assertEqual(kwargs['max_batch_duration_secs'], 1.0)


if __name__ == '__main__':
  unittest.main()
