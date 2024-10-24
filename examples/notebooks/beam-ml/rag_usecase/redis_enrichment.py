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

"""This module implements enrichment classes to implement semantic search on Redis Vector DB.


Redis :Enrichment Handler
-----------------
:class:`RedisEnrichmentHandler` is a ``EnrichmentSourceHandler`` that performs enrichment/search
by fetching the similar text to the user query/prompt from the knowledge base (redis vector DB) and returns
the similar text along with its embeddings as Beam.Row Object.

Example usage::
  redis_handler = RedisEnrichmentHandler(redis_host='127.0.0.1', redis_port=6379)

  pipeline | Enrichment(redis_handler)

No backward compatibility guarantees. Everything in this module is experimental.
"""

import numpy as np
import redis
from redis.commands.search.query import Query

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = [
    'RedisEnrichmentHandler',
]


class RedisEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
    """A handler for :class:`apache_beam.transforms.enrichment.Enrichment`
  transform to interact with redis vector DB.

  Args:
    redis_host (str): Redis Host to connect to redis DB
    redis_port (int): Redis Port to connect to redis DB
    index_name (str): Index Name created for searching in Redis DB
    vector_field (str): vector field to compute similarity score in vector DB
    return_fields (list): returns list of similar text and its embeddings
    hybrid_fields (str): fields to be selected
    k (int): Value of K in KNN algorithm for searching in redis
  """

    def __init__(
            self,
            redis_host: str,
            redis_port: int,
            index_name: str = "embeddings-index",
            vector_field: str = "text_vector",
            return_fields: list = ["id", "title", "url", "text"],
            hybrid_fields: str = "*",
            k: int = 2,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.index_name = index_name
        self.vector_field = vector_field
        self.return_fields = return_fields
        self.hybrid_fields = hybrid_fields
        self.k = k
        self.client = None

    def __enter__(self):
        """connect to the redis DB using redis client."""
        self.client = redis.Redis(host=self.redis_host, port=self.redis_port)

    def __call__(self, request: beam.Row, *args, **kwargs):
        """
    Reads a row from the redis Vector DB and returns
    a `Tuple` of request and response.

    Args:
    request: the input `beam.Row` to enrich.
    """

        # read embedding vector for user query

        embedded_query = request['text']

        # Prepare the Query
        base_query = f'{self.hybrid_fields}=>[KNN {self.k} @{self.vector_field} $vector AS vector_score]'
        query = (
            Query(base_query)
                .return_fields(*self.return_fields)
                .paging(0, self.k)
                .dialect(2)
        )

        params_dict = {"vector": np.array(embedded_query).astype(dtype=np.float32).tobytes()}

        # perform vector search
        results = self.client.ft(self.index_name).search(query, params_dict)

        return beam.Row(text=embedded_query), beam.Row(docs=results.docs)