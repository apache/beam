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
from collections import defaultdict

# test_utils raises unittest.SkipTest when its dependencies are not installed.
from apache_beam.ml.rag.test_utils import parse_chunk_strings
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding


class ParseChunkStringsTest(unittest.TestCase):
  def test_round_trips_chunk_repr(self):
    chunks = [
        Chunk(
            content=Content(text="it's a \"test\""),
            id='query1',
            index=2,
            metadata={
                'enrichment_data': defaultdict(
                    list,
                    {
                        'id': [1, 2],
                        'distance': [1.0, -0.25],
                        'fields': [{
                            'id': 1, 'metadata': {
                                'language': 'en'
                            }, 'ok': True
                        }]
                    })
            },
            embedding=Embedding(
                dense_embedding=[0.1, 0.2, 0.3],
                sparse_embedding=([1, 4], [0.5, 1e-07]))),
        Chunk(content=Content(image=b'\x00\x01'), id='query2'),
    ]

    parsed = parse_chunk_strings([repr(chunk) for chunk in chunks])

    self.assertEqual(parsed, chunks)
    enrichment_data = parsed[0].metadata['enrichment_data']
    self.assertIsInstance(enrichment_data, defaultdict)
    self.assertIs(enrichment_data.default_factory, list)

  def test_parses_chunk_alias(self):
    # EmbeddableItem used to be named Chunk, and expected outputs written
    # before the rename still use the old name.
    parsed = parse_chunk_strings([
        "Chunk(content=Content(text=None), id='query1', index=0, "
        "metadata={'enrichment_data': defaultdict(<class 'list'>, "
        "{'id': [1], 'distance': [1.0]})}, "
        "embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3], "
        "sparse_embedding=None))"
    ])

    self.assertEqual(
        parsed,
        [
            Chunk(
                content=Content(),
                id='query1',
                metadata={
                    'enrichment_data': defaultdict(
                        list, {
                            'id': [1], 'distance': [1.0]
                        })
                },
                embedding=Embedding(dense_embedding=[0.1, 0.2, 0.3]))
        ])

  def test_rejects_values_that_are_not_chunks(self):
    for value in ("Content(text='a')", "[1, 2]", "'Chunk'", "", "Chunk("):
      with self.subTest(value=value):
        with self.assertRaises(ValueError):
          parse_chunk_strings([value])

  def test_does_not_evaluate_code(self):
    # Each of these is a Chunk whose text would be computed by running code.
    # eval() runs most of them even when builtins are removed.
    expressions = [
        "().__class__.__base__.__name__",
        "[c.__name__ for c in ().__class__.__base__.__subclasses__()][0]",
        "(lambda: 'a')()",
        "'a'.upper()",
        "['a'][0]",
        "'a' + 'b'",
        "'a' if True else 'b'",
        "f'{1}'",
        "str(1)",
        "__import__('os').getcwd()",
        "Chunk",
        "list",
        "list()",
    ]
    for expression in expressions:
      with self.subTest(expression=expression):
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
          parse_chunk_strings([f"Chunk(content=Content(text={expression}))"])

  def test_only_calls_constructors_by_name(self):
    for value in ("Chunk.from_text('a')",
                  "Chunk(content=Content())(1)",
                  "Chunk(content=Content(), metadata=defaultdict(dict, {}))"):
      with self.subTest(value=value):
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
          parse_chunk_strings([value])

  def test_error_message_contains_input(self):
    with self.assertRaisesRegex(ValueError, r"Content\(text=\(\)\.__class__\)"):
      parse_chunk_strings(["Chunk(content=Content(text=().__class__))"])

  def test_rejects_argument_unpacking(self):
    for value in ("Chunk(**{'content': Content()})",
                  "Chunk(*[Content()])",
                  "Chunk(content=Content(), metadata={**{}})"):
      with self.subTest(value=value):
        with self.assertRaises(ValueError):
          parse_chunk_strings([value])


if __name__ == '__main__':
  unittest.main()
