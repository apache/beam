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

"""Common imports for generated dataflow client library."""
# pylint:disable=wildcard-import

from __future__ import absolute_import

import pkgutil

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py import *
  from apache_beam.runners.dataflow.internal.clients.dataflow.dataflow_v1b3_messages import *
  from apache_beam.runners.dataflow.internal.clients.dataflow.dataflow_v1b3_client import *
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position

__path__ = pkgutil.extend_path(__path__, __name__)
