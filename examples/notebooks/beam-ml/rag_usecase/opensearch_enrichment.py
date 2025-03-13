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

"""This module implements enrichment classes to implement semantic search on opensearch Vector DB.


opensearch :Enrichment Handler
-----------------
:class:`opensearchEnrichmentHandler` is a ``EnrichmentSourceHandler`` that performs enrichment/search
by fetching the similar text to the user query/prompt from the knowledge base (opensearch vector DB) and returns
the similar text along with its embeddings as Beam.Row Object.

Example usage::
  opensearch_handler = opensearchEnrichmentHandler(opensearch_host='127.0.0.1', opensearch_port=6379)

  pipeline | Enrichment(opensearch_handler)

No backward compatibility guarantees. Everything in this module is experimental.
"""

import logging


from opensearchpy import OpenSearch
from typing import Optional
import os

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

__all__ = [
    'OpenSearchEnrichmentHandler',
]

# Set the logging level to reduce verbose information
import logging

logging.root.setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class OpenSearchEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
    """A handler for :class:`apache_beam.transforms.enrichment.Enrichment`
    transform to interact with opensearch vector DB.
    """
    def __init__(
            self,
            opensearch_host: str,
            opensearch_port: int,
            username: Optional[str],
            password: Optional[str],
            index_name: str = "embeddings-index",
            vector_field: str = "text_vector",
            k: int = 1,
            size: int = 5,
    ):
        """Args:
          opensearch_host (str): opensearch Host to connect to opensearch DB
          opensearch_port (int): opensearch Port to connect to opensearch DB
          index_name (str): Index Name created for searching in opensearch DB
          vector_field (str): vector field to compute similarity score in vector DB
          k (int): Value of K in KNN algorithm for searching in opensearch
        """
        self.opensearch_host = opensearch_host
        self.opensearch_port = opensearch_port
        self.username = username | os.getenv("OPENSEARCH_USERNAME")
        self.password = password | os.getenv("OPENSEARCH_PASSWORD")
        self.index_name = index_name
        self.vector_field = vector_field
        self.k = k
        self.size = size
        self.client = None

        if not self.username or not self.password:
            raise ValueError("Username and password are needed for connecting to Opensearch cluster.")

    def __enter__(self):
        """connect to the opensearch DB using opensearch client.
        """

        if self.client is None:
            http_auth = [self.username, self.password]
            self.client = OpenSearch(hosts=[f'{self.opensearch_host}:{self.opensearch_port}'],
                                     http_auth=http_auth,
                                     verify_certs=False)

    def __call__(self, request: beam.Row, *args, **kwargs):
        """
        Reads a row from the opensearch Vector DB and returns
        a `Tuple` of request and response.

        Args:
        request: the input `beam.Row` to enrich.
        """

        # read embedding vector for user query

        embedded_query = request['text']

        # Prepare the Query
        query = {
            'size': self.size,
            'query': {
                'knn': {
                    self.vector_field: {
                        "vector": embedded_query,
                        "k": self.k
                    }
                }
            }
        }

        # perform vector search
        results = self.client.search(
            body=query,
            index=self.index_name
        )
        logger.info("Enrichment_results", results)

        return beam.Row(text=embedded_query), beam.Row(docs=results)
