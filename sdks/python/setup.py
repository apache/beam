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

from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import warnings
from distutils.errors import DistutilsError
from distutils.version import StrictVersion

# Pylint and isort disagree here.
# pylint: disable=ungrouped-imports
import setuptools
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution
from pkg_resources import normalize_path
from pkg_resources import to_filename
from setuptools import Command
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from setuptools.command.egg_info import egg_info
from setuptools.command.test import test


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

REQUIRED_PACKAGES = [
    # Apache Avro does not follow semantic versioning, so we should not auto
    # upgrade on minor versions. Due to AVRO-2429, Dataflow still
    # requires Avro 1.8.x.
    'avro>=1.8.1,<1.10.0; python_version < "3.0"',
    # Avro 1.9.2 for python3 was broken. The issue was fixed in version 1.9.2.1
    'avro-python3>=1.8.1,!=1.9.2,<1.10.0; python_version >= "3.0"',
    'crcmod>=1.7,<2.0',
    # Dill doesn't have forwards-compatibility guarantees within minor version.
    # Pickles created with a new version of dill may not unpickle using older
    # version of dill. It is best to use the same version of dill on client and
    # server, therefore list of allowed versions is very narrow.
    # See: https://github.com/uqfoundation/dill/issues/341.
    'dill>=0.3.1.1,<0.3.2',
    'fastavro>=0.21.4,<0.24',
    'funcsigs>=1.0.2,<2; python_version < "3.0"',
    'future>=0.18.2,<1.0.0',
    'futures>=3.2.0,<4.0.0; python_version < "3.0"',
    'grpcio>=1.29.0,<2',
    'hdfs>=2.1.0,<3.0.0',
    'httplib2>=0.8,<0.18.0',
    'mock>=1.0.1,<3.0.0',
    'numpy>=1.14.3,<2',
    'pymongo>=3.8.0,<4.0.0',
    'oauth2client>=2.0.1,<4',
    'protobuf>=3.12.2,<4',
    # [BEAM-6287] pyarrow is not supported on Windows for Python 2
    ('pyarrow>=0.15.1,<0.18.0; python_version >= "3.0" or '
     'platform_system != "Windows"'),
    'pydot>=1.2.0,<2',
    'python-dateutil>=2.8.0,<3',
    'pytz>=2018.3',
    # [BEAM-5628] Beam VCF IO is not supported in Python 3.
    'pyvcf>=0.6.8,<0.7.0; python_version < "3.0"',
    # fixes and additions have been made since typing 3.5
    'requests>=2.24.0,<3.0.0',
    'typing>=3.7.0,<3.8.0; python_full_version < "3.5.3"',
    'typing-extensions>=3.7.0,<3.8.0',
    ]

# [BEAM-8181] pyarrow cannot be installed on 32-bit Windows platforms.
if sys.platform == 'win32' and sys.maxsize <= 2**32:
  REQUIRED_PACKAGES = [
      p for p in REQUIRED_PACKAGES if not p.startswith('pyarrow')
  ]

REQUIRED_TEST_PACKAGES = [
    'freezegun>=0.3.12',
    'nose>=1.3.7',
    'nose_xunitmp>=0.4.1',
    'pandas>=0.24.2,<1; python_full_version < "3.5.3"',
    'pandas>=0.25.2,<1; python_full_version >= "3.5.3"',
    'parameterized>=0.7.1,<0.8.0',
    # pyhamcrest==1.10.0 doesn't work on Py2. Beam still supports Py2.
    # See: https://github.com/hamcrest/PyHamcrest/issues/131.
    'pyhamcrest>=1.9,!=1.10.0,<2.0.0',
    'pyyaml>=3.12,<6.0.0',
    'requests_mock>=1.7,<2.0',
    'tenacity>=5.0.2,<6.0',
    'pytest>=4.4.0,<5.0',
    'pytest-xdist>=1.29.0,<2',
    'pytest-timeout>=1.3.3,<2',
    'rsa<4.1; python_version < "3.0"',
    # sqlalchemy is used only for running xlang jdbc test so limit to Py3
    'sqlalchemy>=1.3,<2.0; python_version >= "3.5"',
    # psycopg is used only for running xlang jdbc test so limit to Py3
    'psycopg2-binary>=2.8.5,<3.0.0; python_version >= "3.5"',
    # testcontainers is used only for running xlang jdbc test so limit to Py3
    'testcontainers>=3.0.3,<4.0.0; python_version >= "3.5"',
    ]

GCP_REQUIREMENTS = [
    'cachetools>=3.1.0,<4',
    'google-apitools>=0.5.31,<0.5.32',
    'google-auth>=1.18.0,<2',
    'google-cloud-datastore>=1.7.1,<2',
    'google-cloud-pubsub>=0.39.0,<2',
    # GCP packages required by tests
    'google-cloud-bigquery>=1.6.0,<2',
    'google-cloud-core>=0.28.1,<2',
    'google-cloud-bigtable>=0.31.1,<2',
    'google-cloud-spanner>=1.13.0,<2',
    'grpcio-gcp>=0.2.2,<1',
    # GCP Packages required by ML functionality
    'google-cloud-dlp>=0.12.0,<2',
    'google-cloud-language>=1.3.0,<2',
    'google-cloud-videointelligence>=1.8.0,<2',
    'google-cloud-vision>=0.38.0,<2',
]

INTERACTIVE_BEAM = [
    'facets-overview>=1.0.0,<2',
    'ipython>=5.8.0,<8',
    'ipykernel>=5.2.0,<6',
    'timeloop>=1.0.2,<2',
]

INTERACTIVE_BEAM_TEST = [
    # notebok utils
    'nbformat>=5.0.5,<6',
    'nbconvert>=5.6.1,<6',
    'jupyter-client>=6.1.2,<7',
    # headless chrome based integration tests
    'selenium>=3.141.0,<4',
    'needle>=0.5.0,<1',
    'chromedriver-binary>=83,<84',
    # use a fixed major version of PIL for different python versions
    'pillow>=7.1.1,<8',
]

AWS_REQUIREMENTS = [
    'boto3 >=1.9'
]

AZURE_REQUIREMENTS = [
    'azure-storage-blob >=12.3.2',
    'azure-core >=1.7.0',
]



# We must generate protos after setup_requires are installed.
def generate_protos_first(original_cmd):
  try:
    # See https://issues.apache.org/jira/browse/BEAM-2366
    # pylint: disable=wrong-import-position
    import gen_protos

    class cmd(original_cmd, object):
      def run(self):
        gen_protos.generate_proto_files()
        super(cmd, self).run()
    return cmd
  except ImportError:
    warnings.warn("Could not import gen_protos, skipping proto generation.")
    return original_cmd


python_requires = '>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*'

if sys.version_info.major == 2:
  warnings.warn(
      'You are using the final Apache Beam release with Python 2 support. '
      'New releases of Apache Beam will require Python 3.6 or a newer version.')

if sys.version_info.major == 3 and sys.version_info.minor == 5:
  warnings.warn(
      'You are using the final Apache Beam release with Python 3.5 support. '
      'New releases of Apache Beam will require Python 3.6 or a newer version.')

if sys.version_info.major == 3 and sys.version_info.minor >= 9:
  warnings.warn(
      'This version of Apache Beam has not been sufficiently tested on '
      'Python %s.%s. You may encounter bugs or missing features.' % (
          sys.version_info.major, sys.version_info.minor))

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
    package_data={'apache_beam': [
        '*/*.pyx', '*/*/*.pyx', '*/*.pxd', '*/*/*.pxd', '*/*.h', '*/*/*.h',
        'testing/data/*.yaml', 'portability/api/*.yaml']},
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
        'apache_beam/utils/counters.py',
        'apache_beam/utils/windowed_value.py',
    ]),
    install_requires=REQUIRED_PACKAGES,
    python_requires=python_requires,
    test_suite='nose.collector',
    # BEAM-8840: Do NOT use tests_require or setup_requires.
    extras_require={
        'docs': ['Sphinx>=1.5.2,<2.0'],
        'test': REQUIRED_TEST_PACKAGES,
        'gcp': GCP_REQUIREMENTS,
        'interactive': INTERACTIVE_BEAM,
        'interactive_test': INTERACTIVE_BEAM_TEST,
        'aws': AWS_REQUIREMENTS,
        'azure': AZURE_REQUIREMENTS
    },
    zip_safe=False,
    # PyPI package information.
    classifiers=[
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        # When updating vesion classifiers, also update version warnings
        # above and in apache_beam/__init__.py.
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    license='Apache License, Version 2.0',
    keywords=PACKAGE_KEYWORDS,
    entry_points={
        'nose.plugins.0.10': [
            'beam_test_plugin = test_config:BeamTestPlugin',
        ]},
    cmdclass={
        'build_py': generate_protos_first(build_py),
        'develop': generate_protos_first(develop),
        'egg_info': generate_protos_first(egg_info),
        'test': generate_protos_first(test),
        'mypy': generate_protos_first(mypy),
    },
)
