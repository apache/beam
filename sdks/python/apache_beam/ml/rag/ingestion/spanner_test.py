#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for apache_beam.ml.rag.ingestion.spanner."""

import datetime
import unittest
from typing import Optional

from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.ml.rag.ingestion.spanner import SpannerColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.spanner import _SpannerSchemaBuilder
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding


class SpannerColumnSpecsBuilderTest(unittest.TestCase):
  def test_add_metadata_field_missing_with_convert_fn_returns_none(self):
    specs = (
        SpannerColumnSpecsBuilder().add_metadata_field(
            "created_at", Optional[str],
            convert_fn=lambda ts: ts.isoformat()).build())
    item = EmbeddableItem(content=Content(text="hello"), id="1", metadata={})
    self.assertIsNone(specs[0].value_fn(item))

  def test_add_metadata_field_explicit_none_with_convert_fn_returns_none(self):
    specs = (
        SpannerColumnSpecsBuilder().add_metadata_field(
            "created_at", Optional[str],
            convert_fn=lambda ts: ts.isoformat()).build())
    item = EmbeddableItem(
        content=Content(text="hello"), id="1", metadata={"created_at": None})
    self.assertIsNone(specs[0].value_fn(item))

  def test_add_metadata_field_present_applies_convert_fn(self):
    ts = datetime.datetime(2026, 1, 2, 3, 4, 5)
    specs = (
        SpannerColumnSpecsBuilder().add_metadata_field(
            "created_at", Optional[str],
            convert_fn=lambda v: v.isoformat()).build())
    item = EmbeddableItem(
        content=Content(text="hello"), id="1", metadata={"created_at": ts})
    self.assertEqual(specs[0].value_fn(item), "2026-01-02T03:04:05")

  def test_add_metadata_field_falsy_value_applies_convert_fn(self):
    specs = (
        SpannerColumnSpecsBuilder().add_metadata_field(
            "count", int, convert_fn=lambda x: x + 1).add_metadata_field(
                "label", str, convert_fn=lambda s: f"prefix:{s}").build())
    item = EmbeddableItem(
        content=Content(text="hello"),
        id="1",
        metadata={
            "count": 0, "label": ""
        })
    self.assertEqual(specs[0].value_fn(item), 1)
    self.assertEqual(specs[1].value_fn(item), "prefix:")

  def test_add_metadata_field_non_none_default_applies_convert_fn(self):
    specs = (
        SpannerColumnSpecsBuilder().add_metadata_field(
            "confidence",
            float,
            convert_fn=lambda x: round(float(x), 2),
            default="0.126").build())
    item = EmbeddableItem(content=Content(text="hello"), id="1", metadata={})
    self.assertEqual(specs[0].value_fn(item), 0.13)

  def test_schema_builder_converts_optional_missing_metadata_field(self):
    specs = (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        add_metadata_field(
            "created_at", Optional[str],
            convert_fn=lambda ts: ts.isoformat()).build())
    builder = _SpannerSchemaBuilder("test_embeddings", specs)
    converter = builder.create_converter()
    item = EmbeddableItem(
        content=Content(text="hello"),
        id="1",
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        metadata={})
    record = converter(item)
    self.assertEqual(record.id, "1")
    self.assertEqual(record.embedding, [0.1, 0.2])
    self.assertIsNone(record.created_at)

    coder = registry.get_coder(builder.record_type)
    self.assertIsInstance(coder, RowCoder)
    decoded = coder.decode(coder.encode(record))
    self.assertEqual(decoded, record)


if __name__ == '__main__':
  unittest.main()
