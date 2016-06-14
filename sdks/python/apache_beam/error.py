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

"""Python Dataflow error classes."""


class DataflowError(Exception):
  """Base class for all Dataflow errors."""


class PipelineError(DataflowError):
  """An error in the pipeline object (e.g. a PValue not linked to it)."""


class PValueError(DataflowError):
  """An error related to a PValue object (e.g. value is not computed)."""


class RunnerError(DataflowError):
  """An error related to a Runner object (e.g. cannot find a runner to run)."""


class SideInputError(DataflowError):
  """An error related to a side input to a parallel Do operation."""


class TransformError(DataflowError):
  """An error related to a PTransform object."""
