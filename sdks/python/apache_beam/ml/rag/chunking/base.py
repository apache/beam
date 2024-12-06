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

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransformProvider
from apache_beam.ml.rag.types import Chunk
from typing import Optional
from collections.abc import Callable
import abc
import uuid
import functools

ChunkIdFn = Callable[[Chunk], str]


def create_random_id(chunk: Chunk):
  return str(uuid.uuid4())


def assign_chunk_id(chunk_id_fn: ChunkIdFn, chunk: Chunk):
  chunk.id = chunk_id_fn(chunk)
  return chunk


class ChunkingTransformProvider(MLTransformProvider):
  def __init__(self, chunk_id_fn: Optional[ChunkIdFn] = None):
    self.assign_chunk_id_fn = functools.partial(
        assign_chunk_id,
        chunk_id_fn if chunk_id_fn is not None else create_random_id)

  @abc.abstractmethod
  def get_text_splitter_transform(self) -> beam.DoFn:
    "Return DoFn emits splits for given content."
    NotImplementedError

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (
        "Split document" >>
        self.get_text_splitter_transform().with_output_types(Chunk)
        | "Assign chunk id" >> beam.Map(
            self.assign_chunk_id_fn).with_output_types(Chunk))
