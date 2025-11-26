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

import logging
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding

_LOGGER = logging.getLogger(__name__)

# Default batch size for writing data to Milvus, matching
# JdbcIO.DEFAULT_BATCH_SIZE.
DEFAULT_WRITE_BATCH_SIZE = 1000


@dataclass
class MilvusConnectionParameters:
  """Configurations for establishing connections to Milvus servers.

  Args:
    uri: URI endpoint for connecting to Milvus server in the format
      "http(s)://hostname:port".
    user: Username for authentication. Required if authentication is enabled and
      not using token authentication.
    password: Password for authentication. Required if authentication is enabled
      and not using token authentication.
    db_name: Database Name to connect to. Specifies which Milvus database to
      use. Defaults to 'default'.
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
  """Utility class providing helper methods for Milvus vector db operations."""
  @staticmethod
  def sparse_embedding(
      sparse_vector: Tuple[List[int],
                           List[float]]) -> Optional[Dict[int, float]]:
    if not sparse_vector:
      return None
    # Converts sparse embedding from (indices, values) tuple format to
    # Milvus-compatible values dict format {dimension_index: value, ...}.
    indices, values = sparse_vector
    return {int(idx): float(val) for idx, val in zip(indices, values)}


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
      cleaned_str = re.sub(
          r"defaultdict\(<class 'list'>", "defaultdict(list", raw_str)

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


def retry_with_backoff(
    operation: Callable[[], Any],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_backoff_factor: float = 2.0,
    operation_name: str = "operation",
    exception_types: Tuple[Type[BaseException], ...] = (Exception, )
) -> Any:
  """Executes an operation with retry logic and exponential backoff.

  This is a generic retry utility that can be used for any operation that may
  fail transiently. It retries the operation with exponential backoff between
  attempts.

  Note:
    This utility is designed for one-time setup operations and complements
    Apache Beam's RequestResponseIO pattern. Use retry_with_backoff() for:

    * Establishing client connections in __enter__() methods (e.g., creating
      MilvusClient instances, database connections) before processing elements
    * One-time setup/teardown operations in DoFn lifecycle methods
    * Operations outside of per-element processing where retry is needed

    For per-element operations (e.g., API calls within Caller.__call__),
    use RequestResponseIO which already provides automatic retry with
    exponential backoff, failure handling, caching, and other features.
    See: https://beam.apache.org/documentation/io/built-in/webapis/

  Args:
    operation: Callable that performs the operation to retry. Should return
      the result of the operation.
    max_retries: Maximum number of retry attempts. Default is 3.
    retry_delay: Initial delay in seconds between retries. Default is 1.0.
    retry_backoff_factor: Multiplier for the delay after each retry. Default
      is 2.0 (exponential backoff).
    operation_name: Name of the operation for logging purposes. Default is
      "operation".
    exception_types: Tuple of exception types to catch and retry. Default is
      (Exception,) which catches all exceptions.

  Returns:
    The result of the operation if successful.

  Raises:
    The last exception encountered if all retry attempts fail.

  Example:
    >>> def connect_to_service():
    ...     return service.connect(host="localhost")
    >>> client = retry_with_backoff(
    ...     connect_to_service,
    ...     max_retries=5,
    ...     retry_delay=2.0,
    ...     operation_name="service connection")
  """
  last_exception = None
  for attempt in range(max_retries + 1):
    try:
      result = operation()
      _LOGGER.info(
          "Successfully completed %s on attempt %d",
          operation_name,
          attempt + 1)
      return result
    except exception_types as e:
      last_exception = e
      if attempt < max_retries:
        delay = retry_delay * (retry_backoff_factor**attempt)
        _LOGGER.warning(
            "%s attempt %d failed: %s. Retrying in %.2f seconds...",
            operation_name,
            attempt + 1,
            e,
            delay)
        time.sleep(delay)
      else:
        _LOGGER.error(
            "Failed %s after %d attempts", operation_name, max_retries + 1)
        raise last_exception
