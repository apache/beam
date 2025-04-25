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

from __future__ import absolute_import

import apache_beam as beam

from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle

from typing import Optional
from opensearchpy import OpenSearch

import os
from dotenv import load_dotenv

load_dotenv()

# Set the logging level to reduce verbose information
import logging

logging.root.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

__all__ = ['InsertDocInOpenSearch', 'InsertEmbeddingInOpenSearch']

"""This module implements IO classes to read document in Opensearch.


Insert Doc in OpenSearch:
-----------------
:class:`InsertDocInOpenSearch` is a ``PTransform`` that writes key and values to a
configured sink, and the write is conducted through a Opensearch pipeline.

The ptransform works by getting the first and second elements from the input,
this means that inputs like `[k,v]` or `(k,v)` are valid.

Example usage::

  pipeline | InsertDocInOpenSearch(host='localhost',
                          port=6379,
                          username='admin',
                          password='admin'
                          batch_size=100)


No backward compatibility guarantees. Everything in this module is experimental.
"""


class InsertDocInOpenSearch(PTransform):
    """InsertDocInOpensearch is a ``PTransform`` that writes a ``PCollection`` of
    key, value tuple or 2-element array into a Opensearch server.
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: Optional[str],
                 password: Optional[str],
                 batch_size: int = 100
                 ):
        """
        Args:
        host (str): The opensearch host
        port (int): The opensearch port
        username (str): username of OpenSearch DB
        password (str): password of OpenSearch DB
        batch_size(int): Number of key, values pairs to write at once

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`
        """
        self.host = host
        self.port = port
        self.username = username | os.getenv("OPENSEARCH_USERNAME")
        self.password = password | os.getenv("OPENSEARCH_PASSWORD")
        self._batch_size = batch_size

        if not self.username or not self.password:
            raise ValueError("Username and password are needed for connecting to Opensearch cluster.")

    def expand(self, pcoll):
        return pcoll \
               | "Reshuffle for Opensearch Insert" >> Reshuffle() \
               | "Insert document into Opensearch" >> beam.ParDo(_InsertDocOpenSearchFn(self.host,
                                                                                        self.port,
                                                                                        self.username,
                                                                                        self.password,
                                                                                        self._batch_size)
                                                                 )


class _InsertDocOpenSearchFn(DoFn):
    """Abstract class that takes in Opensearch
    credentials to connect to Opensearch DB
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 batch_size: int = 100
                 ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.batch_size = batch_size

        self.batch_counter = 0
        self.batch = list()

        self.text_col = None

    def finish_bundle(self):
        self._flush()

    def process(self, element, *args, **kwargs):
        self.batch.append(element)
        self.batch_counter += 1
        if self.batch_counter >= self.batch_size:
            self._flush()
        yield element

    def _flush(self):
        if self.batch_counter == 0:
            return

        with _InsertDocOpenSearchSink(self.host, self.port, self.username, self.password) as sink:
            sink.write(self.batch)
            self.batch_counter = 0
            self.batch = list()


class _InsertDocOpenSearchSink(object):
    """Class where we create Opensearch client
    and write insertion logic in Opensearch
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str
                 ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None

    def _create_client(self):
        if self.client is None:
            http_auth = [self.username, self.password]
            self.client = OpenSearch(hosts=[f'{self.host}:{self.port}'],
                                     http_auth=http_auth,
                                     verify_certs=False)

    def write(self, elements):
        self._create_client()
        documents = []
        logger.info(f'Adding Docs in DB: {len(elements)}')
        for element in elements:
            documents.extend([{
                "index": {
                    "_index": "embeddings-index",
                    "_id": str(element["id"]),
                }
            }, {
                "url": element["url"],
                "title": element["title"],
                "text": element["text"],
                "section_id": element["section_id"]
            }])

        self.client.bulk(body=documents, refresh=True)

    def __enter__(self):
        self._create_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()

"""This module implements IO classes to read text Embeddings in Opensearch.
Insert Embedding in Opensearch :
-----------------
:class:`InsertEmbeddingInOpensearch` is a ``PTransform`` that writes key and values to a
configured sink, and the write is conducted through a Opensearch pipeline.

The ptransform works by getting the first and second elements from the input,
this means that inputs like `[k,v]` or `(k,v)` are valid.

Example usage::

  pipeline | WriteToOpensearch(host='localhost',
                          port=6379,
                          batch_size=100)


No backward compatibility guarantees. Everything in this module is experimental.
"""


class InsertEmbeddingInOpenSearch(PTransform):
    """InsertEmbeddingInOpenSearch is a ``PTransform`` that writes a ``PCollection`` of
    key, value tuple or 2-element array into a Opensearch server.
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: Optional[str],
                 password: Optional[str],
                 batch_size: int = 100,
                 embedded_columns: list = []
                 ):
        """
        Args:
        host (str): The Opensearch host
        port (int): The Opensearch port
        username (str): username of OpenSearch DB
        password (str): password of OpenSearch DB
        batch_size(int): Number of key, values pairs to write at once
        embedded_columns (list): list of column whose embedding needs to be generated

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`
        """
        self.host = host
        self.port = port
        self.username = username | os.getenv("OPENSEARCH_USERNAME")
        self.password = password | os.getenv("OPENSEARCH_PASSWORD")
        self.batch_size = batch_size
        self.embedded_columns = embedded_columns

        if not self.username or not self.password:
            raise ValueError("Username and password are needed for connecting to Opensearch cluster.")

    def expand(self, pcoll):
        return pcoll \
               | "Reshuffle for Embedding in Opensearch Insert" >> Reshuffle() \
               | "Write `Embeddings` to Opensearch" >> beam.ParDo(_WriteEmbeddingInOpenSearchFn(self.host,
                                                                                                self.port,
                                                                                                self.username,
                                                                                                self.password,
                                                                                                self.batch_size,
                                                                                                self.embedded_columns))


class _WriteEmbeddingInOpenSearchFn(DoFn):
    """Abstract class that takes in Opensearch  credentials
    to connect to Opensearch DB
    """

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 batch_size: int = 100,
                 embedded_columns: list = []):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.batch_size = batch_size
        self.embedded_columns = embedded_columns

        self.batch_counter = 0
        self.batch = list()

    def finish_bundle(self):
        self._flush()

    def process(self, element, *args, **kwargs):
        self.batch.append(element)
        self.batch_counter += 1
        if self.batch_counter >= self.batch_size:
            self._flush()

    def _flush(self):
        if self.batch_counter == 0:
            return

        with _InsertEmbeddingInOpenSearchSink(self.host, self.port, self.username, self.password,
                                              self.embedded_columns) as sink:
            sink.write(self.batch)

            self.batch_counter = 0
            self.batch = list()


class _InsertEmbeddingInOpenSearchSink(object):
    """Class where we create Opensearch client
    and write text embedding  in Opensearch DB
    """

    def __init__(self, host: str,
                 port: int,
                 username: str,
                 password: str,
                 embedded_columns: list = []):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.embedded_columns = embedded_columns
        self.client = None

    def _create_client(self):
        if self.client is None:
            http_auth = [self.username, self.password]
            self.client = OpenSearch(hosts=[f'{self.host}:{self.port}'],
                                     http_auth=http_auth,
                                     verify_certs=False
                                     )

    def write(self, elements):
        self._create_client()
        documents = []
        logger.info(f'Insert Embeddings in opensearch DB, count={len(elements)}')
        for element in elements:
            doc_update = {
                "url": element["url"],
                "section_id": element["section_id"]
            }

            for k, v in element.items():
                if k in self.embedded_columns:
                    doc_update[f"{k}_vector"] = v

            documents.extend([{
                "update": {
                    "_index": "embeddings-index",
                    "_id": str(element["id"]),
                }
            }, {
                "doc": doc_update
            }])
        response = self.client.bulk(documents)
        if response.get('errors'):
            for item in response['items']:
                if 'error' in item['update']:
                    logger.error(f"Failed to update document ID {item['update']['_id']}: {item['update']['error']}")
        logger.info(f'Insert Embeddings done')

    def __enter__(self):
        self._create_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()