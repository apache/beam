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

"""
Apache Beam SDK for Python
==========================

`Apache Beam <https://beam.apache.org>`_ provides a simple, powerful programming
model for building both batch and streaming parallel data processing pipelines.

The Apache Beam SDK for Python provides access to Apache Beam capabilities
from the Python programming language.

Status
------
The SDK is still early in its development, and significant changes
should be expected before the first stable version.

Overview
--------
The key concepts in this programming model are

* :class:`~apache_beam.pvalue.PCollection`: represents a collection of data,
  which could be bounded or unbounded in size.
* :class:`~apache_beam.transforms.ptransform.PTransform`: represents a
  computation that transforms input PCollections into output PCollections.
* :class:`~apache_beam.pipeline.Pipeline`: manages a directed acyclic graph of
  :class:`~apache_beam.transforms.ptransform.PTransform` s and
  :class:`~apache_beam.pvalue.PCollection` s that is ready for execution.
* :class:`~apache_beam.runners.runner.PipelineRunner`: specifies where and how
  the pipeline should execute.
* :class:`~apache_beam.io.iobase.Read`: read from an external source.
* :class:`~apache_beam.io.iobase.Write`: write to an external data sink.

Typical usage
-------------
At the top of your source file::

  import apache_beam as beam

After this import statement

* Transform classes are available as
  :class:`beam.FlatMap <apache_beam.transforms.core.FlatMap>`,
  :class:`beam.GroupByKey <apache_beam.transforms.core.GroupByKey>`, etc.
* Pipeline class is available as
  :class:`beam.Pipeline <apache_beam.pipeline.Pipeline>`
* Text read/write transforms are available as
  :class:`beam.io.ReadFromText <apache_beam.io.textio.ReadFromText>`,
  :class:`beam.io.WriteToText <apache_beam.io.textio.WriteToText>`.

Examples
--------
The `examples subdirectory
<https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples>`_
has some examples.

"""


import sys


if not (sys.version_info[0] == 2 and sys.version_info[1] == 7):
  raise RuntimeError(
      'The Apache Beam SDK for Python is supported only on Python 2.7. '
      'It is not supported on Python ['+ str(sys.version_info) + '].')

# pylint: disable=wrong-import-position
import apache_beam.internal.pickler

from apache_beam import coders
from apache_beam import io
from apache_beam import typehints
from apache_beam import version
from apache_beam.pipeline import Pipeline
from apache_beam.transforms import *
# pylint: enable=wrong-import-position

__version__ = version.__version__
