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

"""A package defining the syntax and decorator semantics for type-hints."""

# pylint: disable=wildcard-import
from apache_beam.typehints.typehints import *
from apache_beam.typehints.decorators import *
from apache_beam.typehints.batch import *

# pylint: disable=ungrouped-imports
try:
  import pandas as _
except ImportError:
  pass
else:
  from apache_beam.typehints.pandas_type_compatibility import *

try:
  import pyarrow as _
except ImportError:
  pass
else:
  from apache_beam.typehints.arrow_type_compatibility import *
