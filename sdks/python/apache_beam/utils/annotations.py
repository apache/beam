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

import warnings
from functools import partial
from functools import wraps

# produces only the first occurrence of matching warnings,
# regardless of location
warnings.simplefilter("once")


def annotate(label, since, current):
  """Decorates a function with a deprecated or experimental annotation.

  Args:
	  label: the kind of annotation.
	  since: the version that causes the annotation.
	  current: the suggested replacement function.

  Returns:
	  The decorator for the function.
  """
  def _annotate(fnc):
    @wraps(fnc)
    def inner(*args, **kwargs):
      if label == 'deprecated':
        warning_type = DeprecationWarning
      else:
        FutureWarning
      message = '%s is %s' % (fnc.__name__, label)
      if label == 'deprecated':
        message += ' since %s' %since
      message += '. Use %s instead.'%current if current else '.'
      warnings.warn(message, warning_type)
      return fnc(*args, **kwargs)
    return inner
  return _annotate

# Use partial application to customize each annotation.
# 'current' will be optional in both deprecated and experimental
# while 'since' will be mandatory for deprecated.
deprecated = partial(annotate, label='deprecated', current=None)
experimental = partial(annotate, label='experimental', current=None, since=None)
