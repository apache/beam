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

"""Apache Beam SDK for Python setup file."""

import os
import sys
import warnings
from pathlib import Path

# Pylint and isort disagree here.
# pylint: disable=ungrouped-imports
import setuptools
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution
from pkg_resources import normalize_path
from pkg_resources import parse_version
from pkg_resources import to_filename
from setuptools import Command

# pylint: disable=wrong-import-order
# It is recommended to import setuptools prior to importing distutils to avoid
# using legacy behavior from distutils.
# https://setuptools.readthedocs.io/en/latest/history.html#v48-0-0
from distutils.errors import DistutilsError  # isort:skip


class mypy(Command):
  user_options = []

  def initialize_options(self):
    """Abstract method that is required to be overwritten"""

  def finalize_options(self):
    """Abstract method that is required to be overwritten"""

  def get_project_path(self):
    self.run_command('egg_info')

    # Build extensions in-place
    self.reinitialize_command('build_ext', inplace=1)
    self.run_command('build_ext')

    ei_cmd = self.get_finalized_command("egg_info")

    project_path = normalize_path(ei_cmd.egg_base)
    return os.path.join(project_path, to_filename(ei_cmd.egg_name))

  def run(self):
    import subprocess
    args = ['mypy', self.get_project_path()]
    result = subprocess.call(args)
    if result != 0:
      raise DistutilsError("mypy exited with status %d" % result)

RECOMMENDED_MIN_PIP_VERSION = '19.3.0'
try:
  _PIP_VERSION = get_distribution('pip').version
  if parse_version(_PIP_VERSION) < parse_version(RECOMMENDED_MIN_PIP_VERSION):
    warnings.warn(
        "You are using version {0} of pip. " \
        "However, the recommended min version is {1}.".format(
            _PIP_VERSION, RECOMMENDED_MIN_PIP_VERSION
        )
    )
except DistributionNotFound:
  # Do nothing if pip is not found. This can happen when using `Poetry` or
  # `pipenv` package managers.
  pass

REQUIRED_CYTHON_VERSION = '0.28.1'
try:
  _CYTHON_VERSION = get_distribution('cython').version
  if parse_version(_CYTHON_VERSION) < parse_version(REQUIRED_CYTHON_VERSION):
    warnings.warn(
        "You are using version {0} of cython. " \
        "However, version {1} is recommended.".format(
            _CYTHON_VERSION, REQUIRED_CYTHON_VERSION
        )
    )
except DistributionNotFound:
  # do nothing if Cython is not installed
  pass

try:
  # pylint: disable=wrong-import-position
  from Cython.Build import cythonize as cythonize0

  def cythonize(*args, **kwargs):
    import numpy
    extensions = cythonize0(*args, **kwargs)
    for e in extensions:
      e.include_dirs.append(numpy.get_include())
    return extensions
except ImportError:
  cythonize = lambda *args, **kwargs: []


# We must generate protos after setup_requires are installed.
def generate_protos_first():
  try:
    # pylint: disable=wrong-import-position
    import gen_protos
    gen_protos.generate_proto_files()

  except ImportError:
    warnings.warn("Could not import gen_protos, skipping proto generation.")


if sys.version_info.major == 3 and sys.version_info.minor >= 12:
  warnings.warn(
      'This version of Apache Beam has not been sufficiently tested on '
      'Python %s.%s. You may encounter bugs or missing features.' %
      (sys.version_info.major, sys.version_info.minor))

if __name__ == '__main__':
  # In order to find the tree of proto packages, the directory
  # structure must exist before the call to setuptools.find_packages()
  # executes below.
  generate_protos_first()
  # Keep all dependencies inlined in the setup call, otherwise Dependabot won't
  # be able to parse it.
  setuptools.setup(
      ext_modules=cythonize([
          'apache_beam/**/*.pyx',
          'apache_beam/coders/coder_impl.py',
          'apache_beam/metrics/cells.py',
          'apache_beam/metrics/execution.py',
          'apache_beam/runners/common.py',
          'apache_beam/runners/worker/logger.py',
          'apache_beam/runners/worker/opcounters.py',
          'apache_beam/runners/worker/operations.py',
          'apache_beam/transforms/cy_combiners.py',
          'apache_beam/transforms/stats.py',
          'apache_beam/utils/counters.py',
          'apache_beam/utils/windowed_value.py',
      ], language_level=3),
      cmdclass={
          'mypy': mypy,
      },
  )
