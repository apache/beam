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

"""Apache Beam SDK for Python.

Apache Beam <http://cloud.google.com/dataflow/>
provides a simple, powerful programming model for building both batch
and streaming parallel data processing pipelines.

The Apache Beam SDK for Python provides access to Apache Beam capabilities
from the Python programming language.

Status
------
The SDK is still early in its development, and significant changes
should be expected before the first stable version.

Overview
--------
The key concepts in this programming model are

* PCollection:  represents a collection of data, which could be
  bounded or unbounded in size.
* PTransform:  represents a computation that transforms input
  PCollections into output PCollections.
* Pipeline:  manages a directed acyclic graph of PTransforms and
  PCollections that is ready for execution.
* Runner:  specifies where and how the Pipeline should execute.
* Reading and Writing Data:  your pipeline can read from an external
  source and write to an external data sink.

Typical usage
-------------
At the top of your source file::

    import apache_beam as beam

After this import statement

* transform classes are available as beam.FlatMap, beam.GroupByKey, etc.
* Pipeline class is available as beam.Pipeline
* text source/sink classes are available as beam.io.TextFileSource,
  beam.io.TextFileSink

Examples
--------
The examples subdirectory has some examples.

"""


import sys


if sys.version_info.major != 2:
  raise RuntimeError(
      'Dataflow SDK for Python is supported only on Python 2.7. '
      'It is not supported on Python [%s].' % sys.version)

# pylint: disable=wrong-import-position
import apache_beam.internal.pickler

from apache_beam import coders
from apache_beam import io
from apache_beam import typehints
from apache_beam.pipeline import Pipeline
from apache_beam.transforms import *
# pylint: enable=wrong-import-position
