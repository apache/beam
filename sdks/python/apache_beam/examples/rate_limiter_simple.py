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

"""A simple example demonstrating usage of the EnvoyRateLimiter in a Beam
pipeline.
"""

import argparse
import logging
import time

import apache_beam as beam
from apache_beam.io.components.rate_limiter import EnvoyRateLimiter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import shared


class SampleApiDoFn(beam.DoFn):
  """A DoFn that simulates calling an external API with rate limiting."""
  def __init__(self, rls_address, domain, descriptors):
    self.rls_address = rls_address
    self.domain = domain
    self.descriptors = descriptors
    self._shared = shared.Shared()
    self.rate_limiter = None

  def setup(self):
    # Initialize the rate limiter in setup()
    # We use shared.Shared() to ensure only one RateLimiter instance is created
    # per worker and shared across threads.
    def init_limiter():
      logging.info("Connecting to Envoy RLS at %s", self.rls_address)
      return EnvoyRateLimiter(
          service_address=self.rls_address,
          domain=self.domain,
          descriptors=self.descriptors,
          namespace='example_pipeline')

    self.rate_limiter = self._shared.acquire(init_limiter)

  def process(self, element):
    self.rate_limiter.allow()

    # Process the element mock API call
    logging.info("Processing element: %s", element)
    time.sleep(0.1)
    yield element


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--rls_address',
      default='localhost:8081',
      help='Address of the Envoy Rate Limit Service')
  return parser.parse_known_args(argv)


def run(argv=None):
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:
    _ = (
        p
        | 'Create' >> beam.Create(range(100))
        | 'RateLimit' >> beam.ParDo(
            SampleApiDoFn(
                rls_address=known_args.rls_address,
                domain="mongo_cps",
                descriptors=[{
                    "database": "users"
                }])))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
