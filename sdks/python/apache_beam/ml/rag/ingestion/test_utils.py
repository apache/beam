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

import hashlib
import json
from typing import List

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding

VECTOR_SIZE = 768


def row_to_chunk(row) -> Chunk:
  # Parse embedding string back to float list
  embedding_list = [float(x) for x in row.embedding.strip('[]').split(',')]
  return Chunk(
      id=row.id,
      content=Content(text=row.content if hasattr(row, 'content') else None),
      embedding=Embedding(dense_embedding=embedding_list),
      metadata=json.loads(row.metadata) if hasattr(row, 'metadata') else {})


class ChunkTestUtils:
  """Helper functions for generating test Chunks."""
  @staticmethod
  def from_seed(seed: int, content_prefix: str, seed_multiplier: int) -> Chunk:
    """Creates a deterministic Chunk from a seed value."""
    return Chunk(
        id=f"id_{seed}",
        content=Content(text=f"{content_prefix}{seed}"),
        embedding=Embedding(
            dense_embedding=[
                float(seed + i * seed_multiplier) / 100
                for i in range(VECTOR_SIZE)
            ]),
        metadata={"seed": str(seed)})

  @staticmethod
  def get_expected_values(
      range_start: int,
      range_end: int,
      content_prefix: str = "Testval",
      seed_multiplier: int = 1) -> List[Chunk]:
    """Returns a range of test Chunks."""
    return [
        ChunkTestUtils.from_seed(i, content_prefix, seed_multiplier)
        for i in range(range_start, range_end)
    ]


class HashingFn(beam.CombineFn):
  """Hashing function for verification."""
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input):
    accumulator.append(input.content.text if input.content.text else "")
    return accumulator

  def merge_accumulators(self, accumulators):
    merged = []
    for acc in accumulators:
      merged.extend(acc)
    return merged

  def extract_output(self, accumulator):
    sorted_values = sorted(accumulator)
    return hashlib.md5(''.join(sorted_values).encode()).hexdigest()


def generate_expected_hash(num_records: int) -> str:
  chunks = ChunkTestUtils.get_expected_values(0, num_records)
  values = sorted(
      chunk.content.text if chunk.content.text else "" for chunk in chunks)
  return hashlib.md5(''.join(values).encode()).hexdigest()


def key_on_id(chunk):
  return (int(chunk.id.split('_')[1]), chunk)
