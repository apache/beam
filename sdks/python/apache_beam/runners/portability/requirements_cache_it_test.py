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
   requirements.txt. A requirements text is created during runtime with
   package specified in _PACKAGE_IN_REQUIREMENTS_FILE.
"""

import argparse
import logging
import os
import shutil
import tempfile
from importlib.metadata import PackageNotFoundError
from importlib.metadata import distribution

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

_PACKAGE_IN_REQUIREMENTS_FILE = ['matplotlib', 'seaborn']


def verify_packages_from_requirements_file_are_installed(unused_element):
  _PACKAGE_NOT_IN_REQUIREMENTS_FILE = ['torch']
  packages_to_test = _PACKAGE_IN_REQUIREMENTS_FILE + (
      _PACKAGE_NOT_IN_REQUIREMENTS_FILE)
  for package_name in packages_to_test:
    try:
      output = distribution(package_name)
    except PackageNotFoundError as e:  # pylint: disable=unused-variable
      output = None
    if package_name in _PACKAGE_IN_REQUIREMENTS_FILE:
      assert output is not None, ('Please check if package %s is specified'
                                  ' in requirements file' % package_name)
    if package_name in _PACKAGE_NOT_IN_REQUIREMENTS_FILE:
      assert output is None


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  temp_dir = tempfile.mkdtemp()
  requirements_text_path = os.path.join(temp_dir, 'requirements.txt')
  with open(requirements_text_path, 'w') as f:
    f.write('\n'.join(_PACKAGE_IN_REQUIREMENTS_FILE))
  pipeline_options.view_as(
      SetupOptions).requirements_file = requirements_text_path

  with beam.Pipeline(options=pipeline_options) as p:
    ( # pylint: disable=expression-not-assigned
        p
        | beam.Create([None])
        | beam.Map(verify_packages_from_requirements_file_are_installed))
  shutil.rmtree(temp_dir)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
