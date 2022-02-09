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

"""A pipeline to verify the installation of packages specified in the
   requirements.txt.
   python check_requirements_file_packages.py \\
      --runner=PortableRunner \\
      --job_endpoint=embed \\
      --requirements_file=./requirements.txt \\
      --environment_type="DOCKER"
"""

import argparse
import apache_beam as beam
import logging

from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):

  _PACKAGE_IN_REQUIREMENTS_FILE = 'matplotlib'
  _PACKAGE_NOT_IN_REQUIREMENTS_FILE = 'torch'

  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:
    result = (
        p | beam.Create(
            [_PACKAGE_IN_REQUIREMENTS_FILE, _PACKAGE_NOT_IN_REQUIREMENTS_FILE]))

    def assert_modules(p):
      package_name, is_present = p[0], p[1]
      if package_name == _PACKAGE_IN_REQUIREMENTS_FILE:
        assert is_present, ('Please check if package %s is specified'
                           'in requirements file' % package_name)
      elif package_name == _PACKAGE_NOT_IN_REQUIREMENTS_FILE:
        assert not is_present

    def check_module_present(package_name):
      import pkg_resources as pkg
      try:
        output = pkg.get_distribution(package_name)
      except pkg.DistributionNotFound as e:  # pylint: disable=unused-variable
        output = None
      if output is not None:
        return package_name, True
      return package_name, False

    (result | beam.Map(check_module_present) | beam.Map(assert_modules))  # pylint: disable=expression-not-assigned


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
