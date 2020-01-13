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

"""Module to build and run background caching job.

For internal use only; no backwards-compatibility guarantees.

A background caching job is a job that caches events for all unbounded sources
of a given pipeline. With Interactive Beam, one such job is started when a
pipeline run happens (which produces a main job in contrast to the background
caching job) and meets the following conditions:

  #. The pipeline contains unbounded sources.
  #. No such background job is running.
  #. No such background job has completed successfully and the cached events are
     still valid (invalidated when unbounded sources change in the pipeline).

Once started, the background caching job runs asynchronously until it hits some
cache size limit. Meanwhile, the main job and future main jobs from the pipeline
will run using the deterministic replay-able cached events until they are
invalidated.
"""

# pytype: skip-file

from __future__ import absolute_import

import apache_beam as beam
from apache_beam import runners
from apache_beam.runners.interactive import interactive_environment as ie


def attempt_to_run_background_caching_job(runner, user_pipeline, options=None):
  """Attempts to run a background caching job for a user-defined pipeline.

  The pipeline result is automatically tracked by Interactive Beam in case
  future cancellation/cleanup is needed.
  """
  if is_background_caching_job_needed(user_pipeline):
    # Cancel non-terminal jobs if there is any before starting a new one.
    attempt_to_cancel_background_caching_job(user_pipeline)
    # Evict all caches if there is any.
    ie.current_env().cleanup()
    # TODO(BEAM-8335): refactor background caching job logic from
    # pipeline_instrument module to this module and aggregate tests.
    from apache_beam.runners.interactive import pipeline_instrument as instr
    runner_pipeline = beam.pipeline.Pipeline.from_runner_api(
        user_pipeline.to_runner_api(use_fake_coders=True),
        runner,
        options)
    background_caching_job_result = beam.pipeline.Pipeline.from_runner_api(
        instr.pin(runner_pipeline).background_caching_pipeline_proto(),
        runner,
        options).run()
    ie.current_env().set_pipeline_result(user_pipeline,
                                         background_caching_job_result,
                                         is_main_job=False)


def is_background_caching_job_needed(user_pipeline):
  """Determines if a background caching job needs to be started."""
  background_caching_job_result = ie.current_env().pipeline_result(
      user_pipeline, is_main_job=False)
  # Checks if the pipeline contains any source that needs to be cached.
  return (has_source_to_cache(user_pipeline) and
          # Checks if it's the first time running a job from the pipeline.
          (not background_caching_job_result or
           # Or checks if there is no previous job.
           background_caching_job_result.state not in (
               # DONE means a previous job has completed successfully and the
               # cached events are still valid.
               runners.runner.PipelineState.DONE,
               # RUNNING means a previous job has been started and is still
               # running.
               runners.runner.PipelineState.RUNNING) or
           # Or checks if we can invalidate the previous job.
           is_source_to_cache_changed(user_pipeline)))


def has_source_to_cache(user_pipeline):
  """Determines if a user-defined pipeline contains any source that need to be
  cached."""
  from apache_beam.runners.interactive import pipeline_instrument as instr
  # TODO(BEAM-8335): we temporarily only cache replaceable unbounded sources.
  # Add logic for other cacheable sources here when they are available.
  return instr.has_unbounded_sources(user_pipeline)


def attempt_to_cancel_background_caching_job(user_pipeline):
  """Attempts to cancel background caching job for a user-defined pipeline.

  If no background caching job needs to be cancelled, NOOP. Otherwise, cancel
  such job.
  """
  background_caching_job_result = ie.current_env().pipeline_result(
      user_pipeline, is_main_job=False)
  if (background_caching_job_result and
      not ie.current_env().is_terminated(user_pipeline, is_main_job=False)):
    background_caching_job_result.cancel()


def is_source_to_cache_changed(user_pipeline):
  """Determines if there is any change in the sources that need to be cached
  used by the user-defined pipeline.

  Due to the expensiveness of computations and for the simplicity of usage, this
  function is not idempotent because Interactive Beam automatically discards
  previously tracked signature of transforms and tracks the current signature of
  transforms for the user-defined pipeline if there is any change.

  When it's True, there is addition/deletion/mutation of source transforms that
  requires a new background caching job.
  """
  # By default gets empty set if the user_pipeline is first time seen because
  # we can treat it as adding transforms.
  recorded_signature = ie.current_env().get_cached_source_signature(
      user_pipeline)
  current_signature = extract_source_to_cache_signature(user_pipeline)
  is_changed = not current_signature.issubset(recorded_signature)
  # The computation of extract_unbounded_source_signature is expensive, track on
  # change by default.
  if is_changed:
    ie.current_env().set_cached_source_signature(user_pipeline,
                                                 current_signature)
  return is_changed


def extract_source_to_cache_signature(user_pipeline):
  """Extracts a set of signature for sources that need to be cached in the
  user-defined pipeline.

  A signature is a str representation of urn and payload of a source.
  """
  from apache_beam.runners.interactive import pipeline_instrument as instr
  # TODO(BEAM-8335): we temporarily only cache replaceable unbounded sources.
  # Add logic for other cacheable sources here when they are available.
  unbounded_sources_as_applied_transforms = instr.unbounded_sources(
      user_pipeline)
  unbounded_sources_as_ptransforms = set(
      map(lambda x: x.transform, unbounded_sources_as_applied_transforms))
  context, _ = user_pipeline.to_runner_api(
      return_context=True, use_fake_coders=True)
  signature = set(map(lambda transform: str(transform.to_runner_api(context)),
                      unbounded_sources_as_ptransforms))
  return signature
