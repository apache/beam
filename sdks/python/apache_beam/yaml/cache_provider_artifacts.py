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

import logging
import sys
import time

from apache_beam.version import __version__ as beam_version
from apache_beam.yaml import yaml_provider


def cache_provider_artifacts():
  providers_by_id = {}
  for providers in yaml_provider.standard_providers().values():
    for provider in providers:
      # Dedup for better logging.
      providers_by_id[id(provider)] = provider
  for provider in providers_by_id.values():
    t = time.time()
    artifacts = provider.cache_artifacts()
    if artifacts:
      logging.info(
          'Cached %s in %0.03f seconds.', ', '.join(artifacts), time.time() - t)
  if '.dev' not in beam_version:
    # Also cache a base python venv for fast cloning.
    t = time.time()
    artifacts = yaml_provider.PypiExpansionService._create_venv_to_clone(
        sys.executable)
    logging.info('Cached %s in %0.03f seconds.', artifacts, time.time() - t)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  cache_provider_artifacts()
