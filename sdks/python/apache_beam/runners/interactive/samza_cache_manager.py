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

import os

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.runners.interactive.cache_manager import FileBasedCacheManager


class FileWrite(beam.DoFn):

    def __init__(self, path, coder):
        self.path = path
        self.coder = coder

    def process(self, element):
        with open(self.path, 'ab') as f:
            f.write(self.coder.encode(element))
            f.write(b'\n')


class BasicWriteToText(beam.PTransform):

    def __init__(self, path, coder):
        self.do_fn = FileWrite(path, coder)

    def expand(self, pcoll):
        return pcoll | beam.ParDo(self.do_fn)


class SamzaFileBasedCacheManager(FileBasedCacheManager):
    """CacheManager for samza. Uses a simpler caching transform to circumvent
    advanced features not supported by samza's portable API. Currently only
    supports text formatted writes to disk.
    """

    def __init__(self, cache_dir=None, cache_format='text'):
        if cache_format != 'text':
            raise ValueError('The samza cache manager currently only supports text format')
        super(SamzaFileBasedCacheManager, self).__init__(cache_dir, cache_format)
        self._writer_class = BasicWriteToText

    def sink(self, *labels):
        return self._writer_class(
            self._path(*labels), self.load_pcoder(*labels))

    def _path(self, *labels):
        file_path = filesystems.FileSystems.join(self._cache_dir, *labels)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        return file_path

    def _glob_path(self, *labels):
        return self._path(*labels)