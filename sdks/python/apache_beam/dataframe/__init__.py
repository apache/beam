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

"""Beam DataFrame API

- For high-level documentation see
  https://beam.apache.org/documentation/dsls/dataframes/overview/
- :mod:`apache_beam.dataframe.io`: DataFrame I/Os
- :mod:`apache_beam.dataframe.frames`: DataFrame operations
- :mod:`apache_beam.dataframe.convert`: Conversion between
  :class:`~apache_beam.pvalue.PCollection` and
  :class:`~apache_beam.dataframe.frames.DeferredDataFrame`.
- :mod:`apache_beam.dataframe.schemas`: Mapping from Beam Schemas to pandas
  dtypes.
- :mod:`apache_beam.dataframe.transforms`: Use DataFrame operations within a
  Beam pipeline with `DataframeTransform`.
"""

from apache_beam.dataframe.expressions import allow_non_parallel_operations
