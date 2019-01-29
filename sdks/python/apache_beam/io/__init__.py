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

"""A package defining several input sources and output sinks."""

# pylint: disable=wildcard-import
from __future__ import absolute_import

from apache_beam.io.avroio import *
from apache_beam.io.filebasedsink import *
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Sink
from apache_beam.io.iobase import Write
from apache_beam.io.iobase import Writer
from apache_beam.io.parquetio import *
from apache_beam.io.textio import *
from apache_beam.io.tfrecordio import *
from apache_beam.io.range_trackers import *

# Protect against environments where clientslibrary is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp.bigquery import *
  from apache_beam.io.gcp.pubsub import *
  from apache_beam.io.gcp import gcsio
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position
