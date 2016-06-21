#!/usr/bin/env python
#
# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Top-level import for all CLI-related functionality in apitools.

Note that importing this file will ultimately have side-effects, and
may require imports not available in all environments (such as App
Engine). In particular, picking up some readline-related imports can
cause pain.
"""

# pylint:disable=wildcard-import
# pylint:disable=unused-wildcard-import

from apitools.base.py.app2 import *
from apitools.base.py.base_cli import *

try:
    # pylint:disable=no-name-in-module
    from apitools.base.py.internal.cli import *
except ImportError:
    pass
