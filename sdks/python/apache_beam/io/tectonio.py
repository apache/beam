# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information regarding
# copyright ownership.  The ASF licenses this file to You under the
# Apache License, Version 2.0 (the "License"); you may not use this
# file except in compliance with the License.  You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Tecton Stream Ingest sink for Apache Beam.

This module provides functionality to write data to Tecton's feature store
using the Tecton Stream Ingest API.

For more information about Tecton Stream Ingest API, see:
https://docs.tecton.ai/docs/stream-features/stream-ingest-api
"""

from dataclasses import dataclass
import json
import logging
from typing import Any, Dict, List

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn, PTransform
from apache_beam.transforms import Reshuffle

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry


_LOGGER = logging.getLogger(__name__)

# Tecton Stream Ingest API endpoint template.
INGEST_API_ENDPOINT_TEMPLATE = "https://preview.{instance}/ingest"

# Default batch size for writing data to Milvus, matching
# JdbcIO.DEFAULT_BATCH_SIZE.
DEFAULT_BATCH_SIZE = 1000

# Metrics namespace for Tecton Stream Ingest operations.
TECTON_METRICS_NAMESPACE = 'tecton_stream_ingest'

@dataclass
class TectonStreamWriteConfig:
    """Configuration for writing data to Tecton using Stream Ingest API.

    Attributes:
        tecton_instance: Tecton instance domain (e.g., 'explore.tecton.ai')
        api_key: Tecton API key for authentication
        workspace_name: Name of the Tecton workspace
        stream_source_name: Name of the Stream Source
        batch_size: Number of records to batch together. Defaults to 1000.
        max_retries: Maximum number of retries for failed requests. Defaults
          to 3 retries.
        timeout: Request timeout in seconds. Defaults to 30.0 seconds.
        dry_run: Whether to validate but not write data. Defaults to False.
    """
    tecton_instance: str
    api_key: str
    workspace_name: str
    stream_source_name: str
    batch_size: int = DEFAULT_BATCH_SIZE
    max_retries: int = 3
    timeout: float = 30.0
    dry_run: bool = False

    def __post_init__(self):
      if not self.tecton_instance:
        raise ValueError(
          'Please provide a Tecton instance (`tecton_instance`).')
      if not self.api_key:
        raise ValueError('Please provide an API key (`api_key`).')
      if not self.workspace_name:
        raise ValueError('Please provide a workspace name (`workspace_name`).')
      if not self.stream_source_name:
        raise ValueError(
          'Please provide a stream source name (`stream_source_name`).')

class WriteToTectonStream(PTransform):
    """A PTransform for writing data to Tecton using Stream Ingest API.

    This transform writes data to Tecton's feature store using the Stream Ingest
    API. It supports batching, retries, and error handling.

    Example usage:
        tecton_config = TectonStreamWriteConfig(
            tecton_instance='your-tecton-instance',
            api_key='your-api-key',
            workspace_name='your-workspace',
            stream_source_name='your-stream-source'
        )
        pipeline = beam.Pipeline()
        data = pipeline | beam.Create([{'user_id': '123', 'amount': 100.0}])
        data | WriteToTectonStream(tecton_config=tecton_config)
    """

    def __init__(self, tecton_config: TectonStreamWriteConfig):
      super().__init__()
      tecton_instance = tecton_config.tecton_instance
      self.url = INGEST_API_ENDPOINT_TEMPLATE.format(instance=tecton_instance)
      self.tecton_config = tecton_config

    def expand(self, pcoll):
        return (
            pcoll
            | Reshuffle()
            | beam.ParDo(_WriteTectonStreamFn(self.url,self.tecton_config))
        )

class _WriteTectonStreamFn(DoFn):
    """DoFn for writing to Tecton Stream Ingest API."""
    def __init__(self, url: str, tecton_config: TectonStreamWriteConfig):
        """Initialize the DoFn.

        Args:
            url: URL of the Tecton Stream Ingest API
            tecton_config: Configuration for writing data to Tecton
        """
        self.url = url
        self.tecton_config = tecton_config
        self._batch = []

        self._write_requests = Metrics.counter(
            TECTON_METRICS_NAMESPACE, 'write_requests'
        )
        self._write_errors = Metrics.counter(
            TECTON_METRICS_NAMESPACE, 'write_errors'
        )
        self._records_counter = Metrics.counter(
            TECTON_METRICS_NAMESPACE, 'records_written'
        )

    def process(self, element: Dict[str, Any]):
        """Process a single record.

        Args:
            element: Record to write (should be a dictionary)
        """
        self._batch.append(element)
        if len(self._batch) >= self.tecton_config.batch_size:
            self._flush()

    def finish_bundle(self):
        """Flush any remaining records at the end of the bundle."""
        self._flush()

    def _flush(self):
        """Flush the current batch to Tecton."""
        if not self._batch:
            return
        try:
          with _TectonStreamSink(self.url, self.tecton_config) as sink:
              sink.write(self._batch)
              self._records_counter.inc(len(self._batch))
              self._batch = []
        except Exception as e:
            self._write_errors.inc(len(self._batch))
            _LOGGER.error(
                'Failed to write batch of %d records: %s',
                len(self._batch), str(e)
            )
            raise TectonStreamIngestError(f'Failed to write batch: {e}')

    def display_data(self):
        result = super().display_data()
        result["workspace_name"] = self.tecton_config.workspace_name
        result["stream_source_name"] = self.tecton_config.stream_source_name
        result["batch_size"] = self.tecton_config.batch_size
        result["dry_run"] = self.tecton_config.dry_run
        return result


class _TectonStreamSink:
    """Internal sink class for Tecton Stream Ingest API."""

    def __init__(self, url: str, tecton_config: TectonStreamWriteConfig):
        """Initialize the sink.

        Args:
            url: URL of the Tecton Stream Ingest API
            tecton_config: Configuration for writing data to Tecton
        """
        self.url = url
        self.tecton_config = tecton_config
        self._session = None

        self._write_requests = Metrics.counter(
            TECTON_METRICS_NAMESPACE, 'write_requests'
        )

    def __enter__(self):
        """Enter context manager."""
        self._session = requests.Session()
        retry_strategy = Retry(
            total=self.tecton_config.max_retries,
            backoff_factor=1,
            status_forcelist=[
                requests.codes.TOO_MANY_REQUESTS,
                requests.codes.INTERNAL_SERVER_ERROR,
                requests.codes.BAD_GATEWAY,
                requests.codes.SERVICE_UNAVAILABLE,
                requests.codes.GATEWAY_TIMEOUT
            ],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()
        self._session = None

    def write(self, batch: List[Dict[str, Any]]):
        """Write a batch of records to Tecton Stream Ingest API.

        Args:
            batch: List of records to send
        """
        headers = self._build_headers()
        payload = self._build_payload(batch)

        _LOGGER.debug(
            'Sending batch of %d records to %s',
            len(batch), self.url
        )

        try:
            response = self._session.post(
                url=self.url,
                headers=headers,
                json=payload,
                timeout=self.tecton_config.timeout
            )
            self._write_requests.inc(1)
            if response.status_code != requests.codes.OK:
                req_payload = json.dumps(payload, indent=2)
                error_msg = f'HTTP {response.status_code}: {response.text}'
                _LOGGER.error('Tecton Stream Ingest failed: %s', error_msg)
                _LOGGER.error('Request URL: %s', self.url)
                _LOGGER.error(f'Request payload: {req_payload}')
                raise TectonStreamIngestError(error_msg)
        except RequestException as e:
            error_msg = f'Request failed: {str(e)}'
            _LOGGER.error('Tecton Stream Ingest request failed: %s', error_msg)
            _LOGGER.error('Request URL: %s', self.url)
            raise TectonStreamIngestError(error_msg)

        _LOGGER.debug(
            'Successfully sent batch of %d records',
            len(batch)
        )

    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers."""
        return {
            'Authorization': f'Tecton-key {self.tecton_config.api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'Apache-Beam-TectonIO/1.0'
        }

    def _build_payload(self, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build the Stream Ingest API request payload.

        Args:
            batch: List of records to include in the payload

        Returns:
            Dictionary containing the request payload
        """
        formatted_records = []
        for record in batch:
            formatted_records.append({
                'record': record
            })

        return {
            'workspace_name': self.tecton_config.workspace_name,
            'dry_run': self.tecton_config.dry_run,
            'records': {
                self.tecton_config.stream_source_name: formatted_records
            }
        }

class TectonStreamIngestError(Exception):
    """Exception raised for Tecton Stream Ingest errors."""
    pass

