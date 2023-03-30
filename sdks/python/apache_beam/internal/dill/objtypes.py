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

# This code is based on its counterpart in dill==0.3.1.1 distribution,
# which was forked and incorporated in Apache Beam codebase.
# The original source file is copyright and licensed as follows;

# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# Copyright (c) 2016-2019 The Uncertainty Quantification Foundation.
# License: 3-clause BSD.  The full license text is available at:
#  - https://github.com/uqfoundation/dill/blob/master/LICENSE
"""
all Python Standard Library object types (currently: CH 1-15 @ 2.7)
and some other common object types (i.e. numpy.ndarray)

to load more objects and types, use dill.load_types()
"""

# non-local import of dill.objects
from apache_beam.internal.dill import objects
for _type in objects.keys():
    exec("%s = type(objects['%s'])" % (_type,_type))

del objects
try:
    del _type
except NameError:
    pass
