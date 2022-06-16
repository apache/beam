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
from distutils.errors import DistutilsError
from distutils.version import StrictVersion
from pathlib import Path

# Pylint and isort disagree here.
# pylint: disable=ungrouped-imports
import setuptools
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution
from pkg_resources import normalize_path
from pkg_resources import to_filename
from setuptools import Command


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


def get_version():
  global_names = {}
  exec(  # pylint: disable=exec-used
      open(os.path.join(
          os.path.dirname(os.path.abspath(__file__)),
          'apache_beam/version.py')
          ).read(),
      global_names
  )
  return global_names['__version__']


PACKAGE_NAME = 'apache-beam'
PACKAGE_VERSION = get_version()
PACKAGE_DESCRIPTION = 'Apache Beam SDK for Python'
PACKAGE_URL = 'https://beam.apache.org'
PACKAGE_DOWNLOAD_URL = 'https://pypi.python.org/pypi/apache-beam'
PACKAGE_AUTHOR = 'Apache Software Foundation'
PACKAGE_EMAIL = 'dev@beam.apache.org'
PACKAGE_KEYWORDS = 'apache beam'
PACKAGE_LONG_DESCRIPTION = '''
Apache Beam is a unified programming model for both batch and streaming
data processing, enabling efficient execution across diverse distributed
execution engines and providing extensibility points for connecting to
different technologies and user communities.
'''

REQUIRED_PIP_VERSION = '7.0.0'
_PIP_VERSION = get_distribution('pip').version
if StrictVersion(_PIP_VERSION) < StrictVersion(REQUIRED_PIP_VERSION):
  warnings.warn(
      "You are using version {0} of pip. " \
      "However, version {1} is recommended.".format(
          _PIP_VERSION, REQUIRED_PIP_VERSION
      )
  )

REQUIRED_CYTHON_VERSION = '0.28.1'
try:
  _CYTHON_VERSION = get_distribution('cython').version
  if StrictVersion(_CYTHON_VERSION) < StrictVersion(REQUIRED_CYTHON_VERSION):
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
  from Cython.Build import cythonize
except ImportError:
  cythonize = lambda *args, **kwargs: []

# [BEAM-8181] pyarrow cannot be installed on 32-bit Windows platforms.
if sys.platform == 'win32' and sys.maxsize <= 2**32:
  pyarrow_dependency = ''
else:
  pyarrow_dependency = 'pyarrow>=0.15.1,<8.0.0'

# We must generate protos after setup_requires are installed.
def generate_protos_first():
  try:
    # pylint: disable=wrong-import-position
    import gen_protos
    gen_protos.generate_proto_files()

  except ImportError:
    warnings.warn("Could not import gen_protos, skipping proto generation.")


def get_portability_package_data():
  files = []
  portability_dir = Path(__file__).parent / 'apache_beam' / \
                    'portability' / 'api'
  for ext in ['*.pyi', '*.yaml']:
    files.extend(
        str(p.relative_to(portability_dir.parent.parent))
        for p in portability_dir.rglob(ext))

  return files


python_requires = '>=3.7'

if sys.version_info.major == 3 and sys.version_info.minor >= 10:
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
      name=PACKAGE_NAME,
      version=PACKAGE_VERSION,
      description=PACKAGE_DESCRIPTION,
      long_description=PACKAGE_LONG_DESCRIPTION,
      url=PACKAGE_URL,
      download_url=PACKAGE_DOWNLOAD_URL,
      author=PACKAGE_AUTHOR,
      author_email=PACKAGE_EMAIL,
      packages=setuptools.find_packages(),
      package_data={
          'apache_beam': [
              '*/*.pyx',
              '*/*/*.pyx',
              '*/*.pxd',
              '*/*/*.pxd',
              '*/*.h',
              '*/*/*.h',
              'testing/data/*.yaml',
              *get_portability_package_data()
          ]
      },
      ext_modules=cythonize([
          # Make sure to use language_level=3 cython directive in files below.
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
      ]),
      install_requires=[
        # Avro 1.9.2 for python3 was broken.
        # The issue was fixed in version 1.9.2.1
        'crcmod>=1.7,<2.0',
        'orjson<4.0',
        # Dill doesn't have forwards-compatibility guarantees within minor
        # version. Pickles created with a new version of dill may not unpickle
        # using older version of dill. It is best to use the same version of
        # dill on client and server, therefore list of allowed versions is very
        # narrow. See: https://github.com/uqfoundation/dill/issues/341.
        'dill>=0.3.5.1,<0.3.6',
        'cloudpickle>=2.1.0,<3',
        'fastavro>=0.23.6,<2',
        'grpcio>=1.33.1,<2',
        'hdfs>=2.1.0,<3.0.0',
        'httplib2>=0.8,<0.21.0',
        'numpy>=1.14.3,<1.23.0',
        'pymongo>=3.8.0,<4.0.0',
        'protobuf>=3.12.2,<4',
        'proto-plus>=1.7.1,<2',
        'pydot>=1.2.0,<2',
        'python-dateutil>=2.8.0,<3',
        'pytz>=2018.3',
        'requests>=2.24.0,<3.0.0',
        'typing-extensions>=3.7.0',
      # Dynamic dependencies must be specified in a separate list, otherwise
      # Dependabot won't be able to parse the main list. Any dynamic
      # dependencies will not receive updates from Dependabot.
      ] + [pyarrow_dependency],
      python_requires=python_requires,
      # BEAM-8840: Do NOT use tests_require or setup_requires.
      extras_require={
          'docs': [
              'Sphinx>=1.5.2,<6.0',
              # Pinning docutils as a workaround for Sphinx issue:
              # https://github.com/sphinx-doc/sphinx/issues/9727
              'docutils==0.17.1'
          ],
          'test': [
            'freezegun>=0.3.12',
            'joblib>=1.0.1',
            'mock>=1.0.1,<3.0.0',
            'pandas<2.0.0',
            'parameterized>=0.7.1,<0.8.0',
            'pyhamcrest>=1.9,!=1.10.0,<2.0.0',
            'pyyaml>=3.12,<7.0.0',
            'requests_mock>=1.7,<2.0',
            'tenacity>=5.0.2,<6.0',
            'pytest>=4.4.0,<5.0',
            'pytest-xdist>=1.29.0,<2',
            'pytest-timeout>=1.3.3,<2',
            'scikit-learn>=0.20.0',
            'sqlalchemy>=1.3,<2.0',
            'psycopg2-binary>=2.8.5,<3.0.0',
            'testcontainers[mysql]>=3.0.3,<4.0.0',
            'cryptography>=36.0.0',
          ],
          'gcp': [
            'cachetools>=3.1.0,<5',
            'google-apitools>=0.5.31,<0.5.32',
            # NOTE: Maintainers, please do not require google-auth>=2.x.x
            # Until this issue is closed
            # https://github.com/googleapis/google-cloud-python/issues/10566
            'google-auth>=1.18.0,<3',
            'google-auth-httplib2>=0.1.0,<0.2.0',
            'google-cloud-datastore>=1.8.0,<2',
            'google-cloud-pubsub>=2.1.0,<3',
            'google-cloud-pubsublite>=1.2.0,<2',
            # GCP packages required by tests
            'google-cloud-bigquery>=1.6.0,<3',
            'google-cloud-bigquery-storage>=2.6.3',
            'google-cloud-core>=0.28.1,<2',
            'google-cloud-bigtable>=0.31.1,<2',
            'google-cloud-spanner>=1.13.0,<2',
            'grpcio-gcp>=0.2.2,<1',
            # GCP Packages required by ML functionality
            'google-cloud-dlp>=3.0.0,<4',
            'google-cloud-language>=1.3.0,<2',
            'google-cloud-videointelligence>=1.8.0,<2',
            'google-cloud-vision>=0.38.0,<2',
            'google-cloud-recommendations-ai>=0.1.0,<=0.2.0'
          ],
          'interactive': [
            'facets-overview>=1.0.0,<2',
            'google-cloud-dataproc>=3.0.0,<3.2.0',
            # IPython>=8 is not compatible with Python<=3.7
            'ipython>=7,<8;python_version<="3.7"',
            'ipython>=8,<9;python_version>"3.7"',
            'ipykernel>=6,<7',
            'ipywidgets>=7.6.5,<8',
            # Skip version 6.1.13 due to
            # https://github.com/jupyter/jupyter_client/issues/637
            'jupyter-client>=6.1.11,<6.1.13',
            'timeloop>=1.0.2,<2',
          ],
          'interactive_test': [
            # notebok utils
            'nbformat>=5.0.5,<6',
            'nbconvert>=6.2.0,<7',
            # headless chrome based integration tests
            'needle>=0.5.0,<1',
            'chromedriver-binary>=100,<101',
            # use a fixed major version of PIL for different python versions
            'pillow>=7.1.1,<8',
          ],
          'aws': ['boto3 >=1.9'],
          'azure': [
            'azure-storage-blob >=12.3.2',
            'azure-core >=1.7.0',
          ],
          'dataframe': ['pandas>=1.0,<1.5']
      },
      zip_safe=False,
      # PyPI package information.
      classifiers=[
          'Intended Audience :: End Users/Desktop',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          # When updating version classifiers, also update version warnings
          # above and in apache_beam/__init__.py.
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      license='Apache License, Version 2.0',
      keywords=PACKAGE_KEYWORDS,
      cmdclass={
          'mypy': mypy,
      },
  )
