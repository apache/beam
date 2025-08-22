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

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import uuid

from apache_beam.ml.rag.types import Chunk, Content, Embedding


@dataclass
class MilvusConnectionConfig:
  """Configurations for establishing connections to Milvus servers.

  Args:
    uri: URI endpoint for connecting to Milvus server in the format
      "http(s)://hostname:port".
    user: Username for authentication. Required if authentication is enabled and
      not using token authentication.
    password: Password for authentication. Required if authentication is enabled
      and not using token authentication.
    db_name: Database ID to connect to. Specifies which Milvus database to use.
      Defaults to 'default'.
    token: Authentication token as an alternative to username/password.
    timeout: Connection timeout in seconds. Uses client default if None.
    kwargs: Optional keyword arguments for additional connection parameters.
      Enables forward compatibility.
  """
  uri: str
  user: str = field(default_factory=str)
  password: str = field(default_factory=str)
  db_name: str = "default"
  token: str = field(default_factory=str)
  timeout: Optional[float] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.uri:
      raise ValueError("URI must be provided for Milvus connection")

    # Generate unique alias if not provided. One-to-one mapping between alias
    # and connection - each alias represents exactly one Milvus connection.
    if "alias" not in self.kwargs:
      alias = f"milvus_conn_{uuid.uuid4().hex[:8]}"
      self.kwargs["alias"] = alias


class MilvusHelpers:
  """Utility class providing helper methods for Milvus vector db operations.
  
  This class contains static methods for common operations when working with
  Milvus, including data format conversions and parsing utilities for RAG
  (Retrieval-Augmented Generation) applications.
  """
  @staticmethod
  def sparse_embedding(
      vec: Tuple[List[int], List[float]]) -> Optional[Dict[int, float]]:
    if not vec:
      return None
    # Converts sparse embedding from (indices, values) tuple format to
    # Milvus-compatible values dict format {dimension_index: value, ...}.
    indices, values = vec
    return {int(idx): float(val) for idx, val in zip(indices, values)}

  @staticmethod
  def parse_chunk_strings(chunk_str_list: List[str]) -> List[Chunk]:
    parsed_chunks = []

    # Define safe globals and disable built-in functions for safety.
    safe_globals = {
        'Chunk': Chunk,
        'Content': Content,
        'Embedding': Embedding,
        'defaultdict': defaultdict,
        'list': list,
        '__builtins__': {}
    }

    for raw_str in chunk_str_list:
      try:
        # replace "<class 'list'>" with actual list reference.
        cleaned_str = raw_str.replace(
            "defaultdict(<class 'list'>", "defaultdict(list")
        # Evaluate string in restricted environment.
        chunk = eval(cleaned_str, safe_globals)  # pylint: disable=eval-used
        if isinstance(chunk, Chunk):
          parsed_chunks.append(chunk)
        else:
          raise ValueError("Parsed object is not a Chunk instance")
      except Exception as e:
        raise ValueError(f"Error parsing string:\n{raw_str}\n{e}")

    return parsed_chunks


def unpack_dataclass_with_kwargs(dataclass_instance):
  """Unpacks dataclass fields into a flat dict, merging kwargs with precedence.
  
  Args:
    dataclass_instance: Dataclass instance to unpack.
    
  Returns:
    dict: Flattened dictionary with kwargs taking precedence over fields.
  """
  # Create a copy of the dataclass's __dict__.
  params_dict: dict = dataclass_instance.__dict__.copy()

  # Extract the nested kwargs dictionary.
  nested_kwargs = params_dict.pop('kwargs', {})

  # Merge the dictionaries, with nested_kwargs taking precedence
  # in case of duplicate keys.
  return {**params_dict, **nested_kwargs}
