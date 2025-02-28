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

"""Unit tests for BigQuery table definition caching."""

import threading
import time
import unittest
from unittest import mock

from google.cloud import bigquery
from google.api_core import exceptions

from apache_beam.io.gcp.bigquery_cache import BigQueryTableCache


class BigQueryTableCacheTest(unittest.TestCase):
    """Tests for BigQueryTableCache.
    
    These tests verify the caching behavior, thread safety, and error handling
    of the BigQueryTableCache class.
    """

    def setUp(self):
        """Sets up test fixtures before each test method."""
        self.mock_client = mock.create_autospec(bigquery.Client)
        self.mock_table = mock.create_autospec(bigquery.Table)
        self.table_ref = "project.dataset.table"
        self.cache = BigQueryTableCache(ttl_secs=1)  # Short TTL for testing

    def test_get_table_when_not_cached(self):
        """Verify that uncached table references trigger a BigQuery API call."""
        self.mock_client.get_table.return_value = self.mock_table
        
        result = self.cache.get_table(self.table_ref, self.mock_client)
        
        self.assertEqual(result, self.mock_table)
        self.mock_client.get_table.assert_called_once_with(self.table_ref)

    def test_get_table_uses_cache(self):
        """Verify that cached table references don't trigger API calls."""
        self.mock_client.get_table.return_value = self.mock_table
        
        # First call should hit API
        self.cache.get_table(self.table_ref, self.mock_client)
        # Second call should use cache
        result = self.cache.get_table(self.table_ref, self.mock_client)
        
        self.assertEqual(result, self.mock_table)
        self.mock_client.get_table.assert_called_once()  # Only called once

    def test_get_table_expires_cache(self):
        """Verify that expired cache entries are refreshed."""
        self.mock_client.get_table.return_value = self.mock_table
        
        # First call
        self.cache.get_table(self.table_ref, self.mock_client)
        # Wait for TTL to expire
        time.sleep(1.1)
        # Second call should refresh cache
        self.cache.get_table(self.table_ref, self.mock_client)
        
        self.assertEqual(self.mock_client.get_table.call_count, 2)

    def test_concurrent_access(self):
        """Verify thread safety of the cache implementation."""
        self.mock_client.get_table.return_value = self.mock_table
        thread_count = 10
        
        def worker():
            for _ in range(10):  # Each thread makes 10 calls
                self.cache.get_table(self.table_ref, self.mock_client)
        
        threads = [
            threading.Thread(target=worker) for _ in range(thread_count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            
        # First call per thread should hit API, rest should use cache
        self.assertEqual(self.mock_client.get_table.call_count, 1)

    def test_invalid_table_reference(self):
        """Verify validation of table reference format."""
        invalid_refs = [
            "",  # Empty string
            "project.dataset",  # Missing table
            "project..table",  # Empty dataset
            None,  # None value
            "project.dataset.table.extra",  # Too many parts
        ]
        
        for ref in invalid_refs:
            with self.assertRaises(ValueError):
                self.cache.get_table(ref, self.mock_client)

    def test_invalid_ttl(self):
        """Verify validation of TTL parameter."""
        with self.assertRaises(ValueError):
            BigQueryTableCache(ttl_secs=0)
        with self.assertRaises(ValueError):
            BigQueryTableCache(ttl_secs=-1)

    def test_invalidate(self):
        """Verify that invalidate removes entry from cache."""
        self.mock_client.get_table.return_value = self.mock_table
        
        # Cache the table
        self.cache.get_table(self.table_ref, self.mock_client)
        # Invalidate it
        self.cache.invalidate(self.table_ref)
        # Should trigger new API call
        self.cache.get_table(self.table_ref, self.mock_client)
        
        self.assertEqual(self.mock_client.get_table.call_count, 2)

    def test_clear(self):
        """Verify that clear removes all entries from cache."""
        self.mock_client.get_table.return_value = self.mock_table
        refs = [
            "project.dataset.table1",
            "project.dataset.table2",
            "project.dataset.table3"
        ]
        
        # Cache multiple tables
        for ref in refs:
            self.cache.get_table(ref, self.mock_client)
        # Clear cache
        self.cache.clear()
        # Should trigger new API calls
        for ref in refs:
            self.cache.get_table(ref, self.mock_client)
            
        self.assertEqual(self.mock_client.get_table.call_count, len(refs) * 2)

    def test_bigquery_error_handling(self):
        """Verify that BigQuery API errors are properly propagated."""
        # Test NotFound is handled specially
        self.mock_client.get_table.side_effect = exceptions.NotFound("Table not found")
        result = self.cache.get_table(self.table_ref, self.mock_client)
        self.assertIsNone(result)
        self.mock_client.get_table.assert_called_once()

        # Clear cache and reset mock
        self.cache.clear()
        self.mock_client.reset_mock()
        
        # Test permission denied error is propagated
        self.mock_client.get_table.side_effect = exceptions.PermissionDenied("Permission denied")
        with self.assertRaises(exceptions.PermissionDenied):
            self.cache.get_table(self.table_ref, self.mock_client)

        # Clear cache and reset mock
        self.cache.clear()
        self.mock_client.reset_mock()
        
        # Test bad request error is propagated
        self.mock_client.get_table.side_effect = exceptions.BadRequest("Invalid request")
        with self.assertRaises(exceptions.BadRequest):
            self.cache.get_table(self.table_ref, self.mock_client)

        # Clear cache and reset mock
        self.cache.clear()
        self.mock_client.reset_mock()
        
        # Test internal server error is propagated
        self.mock_client.get_table.side_effect = exceptions.InternalServerError("Internal error")
        with self.assertRaises(exceptions.InternalServerError):
            self.cache.get_table(self.table_ref, self.mock_client)


if __name__ == '__main__':
    unittest.main() 