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
import numpy as np

from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle


import redis
from typing import Optional

# Set the logging level to reduce verbose information
import logging

logging.root.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

__all__ = ['InsertDocInRedis', 'InsertEmbeddingInRedis']



"""This module implements IO classes to read write documents in Redis.


Insert Doc in Redis:
-----------------
:class:`InsertDocInRedis` is a ``PTransform`` that writes key and values to a
configured sink, and the write is conducted through a redis pipeline.

The ptransform works by getting the first and second elements from the input,
this means that inputs like `[k,v]` or `(k,v)` are valid.

Example usage::

  pipeline | InsertDocInRedis(host='localhost',
                          port=6379,
                          batch_size=100)
"""


class InsertDocInRedis(PTransform):
    """InsertDocInRedis is a ``PTransform`` that writes a ``PCollection`` of
    key, value tuple or 2-element array into a redis server.
    """

    def __init__(self,
                 host: str,
                 port: int,
                 command: Optional[str] = None,
                 batch_size: int = 100
                 ):

        """

        Args:
        host (str): The redis host
        port (int): The redis port
        command (str): command to be executed with redis client
        batch_size(int): Number of key, values pairs to write at once

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`

        """

        self._host = host
        self._port = port
        self._command = command
        self._batch_size = batch_size

    def expand(self, pcoll):
        return pcoll \
               | "Reshuffle for Redis Insert" >> Reshuffle() \
               | "Insert document into Redis" >> beam.ParDo(_InsertDocRedisFn(self._host,
                                                                              self._port,
                                                                              self._command,
                                                                              self._batch_size)
                                                            )


class _InsertDocRedisFn(DoFn):
    """Abstract class that takes in redis
    credentials to connect to redis DB
    """

    def __init__(self,
                 host: str,
                 port: int,
                 command: Optional[str] = None,
                 batch_size: int = 100
                 ):
        self.host = host
        self.port = port
        self.command = command
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

        with _InsertDocRedisSink(self.host, self.port) as sink:

            if not self.command:
                sink.write(self.batch)

            else:
                sink.execute_command(self.command, self.batch)

            self.batch_counter = 0
            self.batch = list()


class _InsertDocRedisSink(object):
    """Class where we create redis client
    and write insertion logic in redis
    """

    def __init__(self,
                 host: str,
                 port: int
                 ):
        self.host = host
        self.port = port
        self.client = None

    def _create_client(self):
        if self.client is None:
            self.client = redis.Redis(host=self.host,
                                      port=self.port)

    def write(self, elements):
        self._create_client()
        with self.client.pipeline() as pipe:
            logger.info(f'Inserting documents in Redis. Total docs: {len(elements)}')
            for element in elements:
                doc_key = f"doc_{str(element['id'])}_section_{str(element['section_id'])}"
                for k, v in element.items():
                    logger.debug(f'Inserting doc_key={doc_key}, key={k}, value={v}')
                    pipe.hset(name=doc_key, key=k, value=v)

            pipe.execute()
            logger.info(f'Inserting documents complete.')


    def execute_command(self, command, elements):
        self._create_client()
        with self.client.pipeline() as pipe:
            for element in elements:
                k, v = element
                pipe.execute_command(command, k, v)
            pipe.execute()

    def __enter__(self):
        self._create_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()


"""This module implements IO classes to read write text Embeddings in Redis.


Insert Embedding in Redis :
-----------------
:class:`InsertEmbeddingInRedis` is a ``PTransform`` that writes key and values to a
configured sink, and the write is conducted through a redis pipeline.

The ptransform works by getting the first and second elements from the input,
this means that inputs like `[k,v]` or `(k,v)` are valid.

Example usage::

  pipeline | InsertEmbeddingInRedis(host='localhost',
                          port=6379,
                          batch_size=100)
"""


class InsertEmbeddingInRedis(PTransform):
    """WriteToRedis is a ``PTransform`` that writes a ``PCollection`` of
    key, value tuple or 2-element array into a redis server.
    """

    def __init__(self,
                 host: str,
                 port: int,
                 command: Optional[str] = None,
                 batch_size: int = 100,
                 embedded_columns: list = []
                 ):

        """

        Args:
        host (str): The redis host
        port (int): The redis port
        command (str): command to be executed with redis client
        batch_size (int): Number of key, values pairs to write at once
        embedded_columns (list): list of column whose embedding needs to be generated

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`

        """

        self._host = host
        self._port = port
        self._command = command
        self._batch_size = batch_size
        self.embedded_columns = embedded_columns

    def expand(self, pcoll):
        return pcoll \
               | "Reshuffle for Embedding in Redis Insert" >> Reshuffle() \
               | "Write `Embeddings` to Redis" >> beam.ParDo(_WriteEmbeddingInRedisFn(self._host,
                                                                                      self._port,
                                                                                      self._command,
                                                                                      self._batch_size,
                                                                                      self.embedded_columns))


class _WriteEmbeddingInRedisFn(DoFn):
    """Abstract class that takes in redis  credentials
    to connect to redis DB
    """

    def __init__(self,
                 host: str,
                 port: int,
                 command: Optional[str] = None,
                 batch_size: int = 100,
                 embedded_columns: list = []
                 ):
        self.host = host
        self.port = port
        self.command = command
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

        with _InsertEmbeddingInRedisSink(self.host, self.port, self.embedded_columns) as sink:

            if not self.command:
                sink.write(self.batch)

            else:
                sink.execute_command(self.command, self.batch)

            self.batch_counter = 0
            self.batch = list()


class _InsertEmbeddingInRedisSink(object):
    """Class where we create redis client
    and write text embedding  in redis DB
    """

    def __init__(self,
                 host: str,
                 port: int,
                 embedded_columns: list = []
                 ):
        self.host = host
        self.port = port
        self.client = None
        self.embedded_columns = embedded_columns

    def _create_client(self):
        if self.client is None:
            self.client = redis.Redis(host=self.host,
                                      port=self.port)

    def write(self, elements):
        self._create_client()
        with self.client.pipeline() as pipe:
            for element in elements:
                doc_key = f"doc_{str(element['id'])}_section_{str(element['section_id'])}"
                for k, v in element.items():
                    if k in self.embedded_columns:
                        v = np.array(v, dtype=np.float32).tobytes()
                        pipe.hset(name=doc_key, key=f'{k}_vector', value=v)
            pipe.execute()

    def execute_command(self, command, elements):
        self._create_client()
        with self.client.pipeline() as pipe:
            for element in elements:
                k, v = element
                pipe.execute_command(command, k, v)
            pipe.execute()

    def __enter__(self):
        self._create_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()