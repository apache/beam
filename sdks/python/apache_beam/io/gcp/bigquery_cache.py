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

"""BigQuery table definition caching implementation."""

import threading
import time
from typing import Dict, Optional, Tuple

from google.cloud import bigquery
from google.api_core import exceptions


class BigQueryTableCache:
  """A thread-safe cache for BigQuery table definitions.
  
  This cache helps reduce the number of API calls to BigQuery by storing table
  definitions that have been previously fetched. It is particularly useful in
  scenarios where the same table is accessed multiple times during pipeline
  execution.

  The cache is thread-safe and supports automatic expiration of cached entries
  based on a configurable TTL (Time To Live).

  Attributes:
    _ttl_secs: Time in seconds before a cached entry expires.
    _cache: Internal dictionary storing table definitions and their timestamps.
    _lock: Threading lock for thread-safe operations.
  """

  def __init__(self, ttl_secs: int = 300):
    """Initialize the BigQuery table cache.
    
    Args:
      ttl_secs: Time in seconds before a cached entry expires. 
                Must be positive. Defaults to 300 (5 minutes).
                
    Raises:
      ValueError: If ttl_secs is not positive.
    """
    if ttl_secs <= 0:
      raise ValueError("TTL must be positive")
    self._ttl_secs = ttl_secs
    self._cache: Dict[str, Tuple[Optional[bigquery.Table], float]] = {}
    self._lock = threading.Lock()

  def _validate_table_reference(self, table_reference: str) -> None:
    """Validates the format of a table reference string.
    
    Args:
      table_reference: String in format 'project.dataset.table'.
        
    Raises:
      ValueError: If table_reference format is invalid.
    """
    if not table_reference or not isinstance(table_reference, str):
      raise ValueError("Invalid table reference: must be non-empty string")

    parts = table_reference.split('.')
    if len(parts) != 3 or not all(parts):
      raise ValueError(
          "Invalid table reference: must be in format 'project.dataset.table'")

  def get_table(self, table_reference: str,
                client: bigquery.Client) -> Optional[bigquery.Table]:
    """Retrieves a table from cache or fetches it from BigQuery.
    
    Args:
      table_reference: String in format 'project.dataset.table'.
      client: BigQuery client instance to use for API calls.
        
    Returns:
      The requested table or None if it doesn't exist.
        
    Raises:
      ValueError: If table_reference format is invalid.
      google.api_core.exceptions.PermissionDenied: If the request is denied.
      google.api_core.exceptions.BadRequest: If the request is invalid.
      google.api_core.exceptions.InternalServerError: If BigQuery has an internal error.
      Any other exceptions from the BigQuery API except NotFound.
    """
    self._validate_table_reference(table_reference)

    with self._lock:
      # Check if we have a cached entry that hasn't expired
      if table_reference in self._cache:
        table, timestamp = self._cache[table_reference]
        if time.time() - timestamp < self._ttl_secs:
          return table

      # Fetch from BigQuery
      try:
        table = client.get_table(table_reference)
        self._cache[table_reference] = (table, time.time())
        return table
      except exceptions.NotFound:
        # Cache the not found result
        self._cache[table_reference] = (None, time.time())
        return None
      except (exceptions.PermissionDenied, exceptions.BadRequest,
              exceptions.InternalServerError):
        # Don't cache errors that might be transient or fixable
        raise

  def invalidate(self, table_reference: str) -> None:
    """Removes a table from the cache.
    
    Args:
      table_reference: String in format 'project.dataset.table'.
        
    Raises:
      ValueError: If table_reference format is invalid.
    """
    self._validate_table_reference(table_reference)
    with self._lock:
      self._cache.pop(table_reference, None)

  def clear(self) -> None:
    """Clears all entries from the cache."""
    with self._lock:
      self._cache.clear() 