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

# Standard imports
import threading
import time
import unittest
from typing import Dict, Optional, Tuple

# Third-party imports
import mock
from google.cloud import bigquery
from google.api_core import exceptions

# Beam imports
from apache_beam.io.gcp.bigquery import BigQueryTableCache
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.io.gcp.bigquery_tools import TableReference

class BigQueryTableCacheTest(unittest.TestCase):
    """Tests for BigQueryTableCache.
    
    These tests verify the caching behavior, thread safety, and error handling
    of the BigQueryTableCache class. The tests cover cache hits/misses,
    expiration, concurrent access, and various error conditions.
    """

    def setUp(self):
        """Sets up test fixtures before each test method."""
        self.mock_client = mock.Mock()
        self.mock_table = mock.Mock()
        self.mock_client.get_table.return_value = self.mock_table
        self.cache = BigQueryTableCache(ttl_secs=1)  # Short TTL for testing
        self.table_ref = "project.dataset.table"

    def test_get_table_when_not_cached(self):
        """Tests that uncached table references trigger a BigQuery API call.
        
        This test verifies that when a table is not in the cache, the code
        properly calls the BigQuery API to fetch it.
        """
        table_ref = "project.dataset.table"
        mock_table = mock.Mock()
        mock_client = mock.Mock()
        mock_client.get_table.return_value = mock_table
        
        cache = BigQueryTableCache()
        result = cache.get_table(table_ref, mock_client)
        
        self.assertEqual(result, mock_table)
        mock_client.get_table.assert_called_once_with(table_ref)

    def test_concurrent_access(self):
        """Tests thread safety of the cache implementation.
        
        Verifies that concurrent access to the cache by multiple threads
        works correctly and maintains consistency.
        """
        table_ref = "project.dataset.table"
        mock_table = mock.Mock()
        mock_client = mock.Mock()
        mock_client.get_table.return_value = mock_table
        
        # Create a single cache instance to be shared
        cache = BigQueryTableCache()
        
        def worker():
            for _ in range(100):
                result = cache.get_table(table_ref, mock_client)
                self.assertEqual(result, mock_table)
        
        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should only call get_table once despite concurrent access
        mock_client.get_table.assert_called_once_with(table_ref)

    def test_bigquery_error_handling(self):
        """Tests proper propagation of BigQuery API errors."""
        error_cases = [
            (exceptions.NotFound, "Table not found"),
            (exceptions.PermissionDenied, "Permission denied"),
            (exceptions.BadRequest, "Invalid request"),
            (exceptions.ServerError, "Server error"),
            (exceptions.Timeout, "Request timeout"),
            (exceptions.TooManyRequests, "Quota exceeded"),
            (exceptions.InternalServerError, "Internal error")
        ]
        
        for error_class, error_message in error_cases:
            self.mock_client.get_table.side_effect = error_class(error_message)
            with self.assertRaises(error_class) as context:
                self.cache.get_table(self.table_ref, self.mock_client)
            self.assertIn(error_message, str(context.exception))

    def test_invalid_table_reference(self):
        """Tests validation of table reference format."""
        invalid_refs = [
            "",
            None,
            "invalid",
            "project.dataset",  # missing table
            "project..table",   # empty dataset
            ".dataset.table",   # empty project
            "project.dataset.", # empty table
            "too.many.dots.here"
        ]
        
        for invalid_ref in invalid_refs:
            with self.assertRaises(ValueError) as context:
                self.cache.get_table(invalid_ref, self.mock_client)
            self.assertIn("Invalid table reference", str(context.exception)) 

    def test_invalid_ttl(self):
        """Tests validation of TTL parameter."""
        invalid_ttls = [0, -1, -100]
        
        for invalid_ttl in invalid_ttls:
            with self.assertRaises(ValueError) as context:
                BigQueryTableCache(ttl_secs=invalid_ttl)
            self.assertIn("TTL must be positive", str(context.exception)) 

    def test_get_table_uses_cache(self):
        """Tests that cached table references don't trigger API calls."""
        # First call should hit BigQuery
        first_result = self.cache.get_table(self.table_ref, self.mock_client)
        self.assertEqual(first_result, self.mock_table)
        
        # Second call should use cache
        second_result = self.cache.get_table(self.table_ref, self.mock_client)
        self.assertEqual(second_result, self.mock_table)
        
        # Should only call get_table once
        self.mock_client.get_table.assert_called_once_with(self.table_ref) 

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

    def get_table(
            self,
            table_reference: str,
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
            except (
                exceptions.PermissionDenied,
                exceptions.BadRequest,
                exceptions.InternalServerError
            ) as e:
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