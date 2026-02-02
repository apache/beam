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

"""Singleton providing access to pipeline options during graph construction.

This module provides a lightweight singleton that holds the full
PipelineOptions set during Pipeline.__init__.
"""

from typing import Optional

from apache_beam.options.pipeline_options import PipelineOptions


class PipelineConstructionOptions:
  """Holds the current pipeline's options during graph construction.

  Set during Pipeline.__init__.
  """
  options: Optional[PipelineOptions] = None


pipeline_construction_options = PipelineConstructionOptions()
