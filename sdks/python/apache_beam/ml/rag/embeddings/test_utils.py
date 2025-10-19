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

"""Utility functions for RAG embeddings."""

from apache_beam.ml.rag.types import Chunk


def chunk_approximately_equals(expected, actual):
  """Compare embeddings allowing for numerical differences."""
  if not isinstance(expected, Chunk) or not isinstance(actual, Chunk):
    return False

  return (
      expected.id == actual.id and expected.metadata == actual.metadata and
      expected.content == actual.content and
      len(expected.embedding.dense_embedding) == len(
          actual.embedding.dense_embedding) and
      all(isinstance(x, float) for x in actual.embedding.dense_embedding))
