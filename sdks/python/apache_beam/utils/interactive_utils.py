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

"""Common interactive utility module.

For experimental usage only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

import logging

_LOGGER = logging.getLogger(__name__)


def is_in_ipython():
  """Determines if current code is executed within an ipython session."""
  try:
    from IPython import get_ipython  # pylint: disable=import-error
    if get_ipython():
      return True
    return False
  except ImportError:
    # If dependencies are not available, then not interactive for sure.
    return False
  except (KeyboardInterrupt, SystemExit):
    raise
  except:  # pylint: disable=bare-except
    _LOGGER.info(
        'Unexpected error occurred, treated as not in IPython.', exc_info=True)
    return False


def is_in_notebook():
  """Determines if current code is executed from an ipython notebook.

  If is_in_notebook() is True, then is_in_ipython() must also be True.
  """
  is_in_notebook = False
  if is_in_ipython():
    # The import and usage must be valid under the execution path.
    from IPython import get_ipython
    if 'IPKernelApp' in get_ipython().config:
      is_in_notebook = True
  return is_in_notebook


def alter_label_if_ipython(transform, pvalueish):
  """Alters the label to an interactive label with ipython prompt metadata
  prefixed for the given transform if the given pvalueish belongs to a
  user-defined pipeline and current code execution is within an ipython kernel.
  Otherwise, noop.

  A label is either a user-defined or auto-generated str name of a PTransform
  that is unique within a pipeline. If current environment is_in_ipython(), Beam
  can implicitly create interactive labels to replace labels of top-level
  PTransforms to be applied. The label is formatted as:
  `Cell {prompt}: {original_label}`.
  """
  if is_in_ipython():
    from apache_beam.runners.interactive import interactive_environment as ie
    # Tracks user defined pipeline instances in watched scopes so that we only
    # alter labels for any transform to pvalueish belonging to those pipeline
    # instances, excluding any transform to be applied in other pipeline
    # instances the Beam SDK creates implicitly.
    ie.current_env().track_user_pipelines()
    from IPython import get_ipython
    prompt = get_ipython().execution_count
    pipeline = _extract_pipeline_of_pvalueish(pvalueish)
    if (pipeline
        # We only alter for transforms to be applied to user-defined pipelines
        # at pipeline construction time.
        and pipeline in ie.current_env().tracked_user_pipelines):
      transform.label = '[{}]: {}'.format(prompt, transform.label)


def _extract_pipeline_of_pvalueish(pvalueish):
  """Extracts the pipeline that the given pvalueish belongs to."""
  if isinstance(pvalueish, tuple) and len(pvalueish) > 0:
    pvalue = pvalueish[0]
  elif isinstance(pvalueish, dict) and len(pvalueish) > 0:
    pvalue = next(iter(pvalueish.values()))
  else:
    pvalue = pvalueish
  if hasattr(pvalue, 'pipeline'):
    return pvalue.pipeline
  return None
