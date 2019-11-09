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

import contextlib
import glob
import inspect
import os
import platform
import re
import shutil
import subprocess
import sys
import warnings
from distutils import log
from distutils.errors import DistutilsError
from distutils.version import StrictVersion

# Pylint and isort disagree here.
# pylint: disable=ungrouped-imports
import setuptools
from pkg_resources import DistributionNotFound
from pkg_resources import get_distribution
from pkg_resources import normalize_path
from pkg_resources import to_filename
from pkg_resources import resource_filename
from setuptools import Command
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist


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

# Currently all compiled modules are optional  (for performance only).
if platform.system() == 'Windows':
  # Windows doesn't always provide int64_t.
  cythonize = lambda *args, **kwargs: []
else:
  try:
    # pylint: disable=wrong-import-position
    from Cython.Build import cythonize
  except ImportError:
    cythonize = lambda *args, **kwargs: []

REQUIRED_PACKAGES = [
    'avro>=1.8.1,<2.0.0; python_version < "3.0"',
    'avro-python3>=1.8.1,<2.0.0; python_version >= "3.0"',
    'crcmod>=1.7,<2.0',
    # Dill doesn't have forwards-compatibility guarantees within minor version.
    # Pickles created with a new version of dill may not unpickle using older
    # version of dill. It is best to use the same version of dill on client and
    # server, therefore list of allowed versions is very narrow.
    # See: https://github.com/uqfoundation/dill/issues/341.
    'dill>=0.3.1.1,<0.3.2',
    'fastavro>=0.21.4,<0.22',
    'funcsigs>=1.0.2,<2; python_version < "3.0"',
    'future>=0.16.0,<1.0.0',
    'futures>=3.2.0,<4.0.0; python_version < "3.0"',
    'grpcio>=1.12.1,<2',
    'hdfs>=2.1.0,<3.0.0',
    'httplib2>=0.8,<=0.12.0',
    'mock>=1.0.1,<3.0.0',
    'numpy>=1.14.3,<2',
    'pymongo>=3.8.0,<4.0.0',
    'oauth2client>=2.0.1,<4',
    'protobuf>=3.5.0.post1,<4',
    # [BEAM-6287] pyarrow is not supported on Windows for Python 2
    ('pyarrow>=0.15.1,<0.16.0; python_version >= "3.0" or '
     'platform_system != "Windows"'),
    'pydot>=1.2.0,<2',
    'python-dateutil>=2.8.0,<3',
    'pytz>=2018.3',
    # [BEAM-5628] Beam VCF IO is not supported in Python 3.
    'pyvcf>=0.6.8,<0.7.0; python_version < "3.0"',
    # fixes and additions have been made since typing 3.5
    'typing>=3.7.0,<3.8.0; python_version < "3.8.0"',
    'typing-extensions>=3.7.0,<3.8.0; python_version < "3.8.0"',
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
    'pandas>=0.23.4,<0.25',
    'parameterized>=0.6.0,<0.8.0',
    # pyhamcrest==1.10.0 doesn't work on Py2. Beam still supports Py2.
    # See: https://github.com/hamcrest/PyHamcrest/issues/131.
    'pyhamcrest>=1.9,!=1.10.0,<2.0.0',
    'pyyaml>=3.12,<6.0.0',
    'requests_mock>=1.7,<2.0',
    'tenacity>=5.0.2,<6.0',
    'pytest>=4.4.0,<5.0',
    'pytest-xdist>=1.29.0,<2',
    ]

GCP_REQUIREMENTS = [
    'cachetools>=3.1.0,<4',
    'google-apitools>=0.5.28,<0.5.29',
    # [BEAM-4543] googledatastore is not supported in Python 3.
    'googledatastore>=7.0.1,<7.1; python_version < "3.0"',
    'google-cloud-datastore>=1.7.1,<1.8.0',
    'google-cloud-pubsub>=0.39.0,<1.1.0',
    # GCP packages required by tests
    'google-cloud-bigquery>=1.6.0,<1.18.0',
    'google-cloud-core>=0.28.1,<2',
    'google-cloud-bigtable>=0.31.1,<1.1.0',
    # [BEAM-4543] googledatastore is not supported in Python 3.
    'proto-google-cloud-datastore-v1>=0.90.0,<=0.90.4; python_version < "3.0"',
]

INTERACTIVE_BEAM = [
    'facets-overview>=1.0.0,<2',
    'ipython>=5.8.0,<6',
    # jsons is supported by Python 3.5.3+.
    'jsons>=1.0.0,<2; python_version >= "3.5.3"',
    'timeloop>=1.0.2,<2',
]
AWS_REQUIREMENTS = [
    'boto3 >=1.9'
]

BEAM_PROTO_PATHS = [
    os.path.join('..', '..', 'model', 'pipeline', 'src', 'main', 'proto'),
    os.path.join('..', '..', 'model', 'job-management', 'src', 'main', 'proto'),
    os.path.join('..', '..', 'model', 'fn-execution', 'src', 'main', 'proto'),
]

PYTHON_PROTO_OUTPUT_PATH = os.path.join('apache_beam', 'portability', 'api')

MODEL_RESOURCES = [
    os.path.normpath('../../model/fn-execution/src/main/resources'\
            + '/org/apache/beam/model/fnexecution/v1/standard_coders.yaml'),
]


def generate_urn_files(log, out_dir):
  """
  Create python files with statically defined URN constants.

  Creates a <proto>_pb2_urn.py file for each <proto>_pb2.py file that contains
  an enum type.

  This works by importing each api.<proto>_pb2 module created by `protoc`,
  inspecting the module's contents, and generating a new side-car urn module.
  This is executed at build time rather than dynamically on import to ensure
  that it is compatible with static type checkers like mypy.
  """
  import google.protobuf.message as message
  import google.protobuf.pyext._message as pyext_message

  class Context(object):
    INDENT = '  '
    CAP_SPLIT = re.compile('([A-Z][^A-Z]*|^[a-z]+)')

    def __init__(self, indent=0):
      self.lines = []
      self.imports = set()
      self.empty_types = set()
      self._indent = indent

    @contextlib.contextmanager
    def indent(self):
      self._indent += 1
      yield
      self._indent -= 1

    def prepend(self, s):
      if s:
        self.lines.insert(0, (self.INDENT * self._indent) + s + '\n')
      else:
        self.lines.insert(0, '\n')

    def line(self, s):
      if s:
        self.lines.append((self.INDENT * self._indent) + s + '\n')
      else:
        self.lines.append('\n')

    def import_type(self, typ):
      modname = typ.__module__
      if modname in ('__builtin__', 'builtin'):
        return typ.__name__
      else:
        self.imports.add(modname)
        return modname + '.' + typ.__name__

    @staticmethod
    def is_message_type(obj):
      return isinstance(obj, type) and \
             issubclass(obj, message.Message)

    @staticmethod
    def is_enum_type(obj):
      return type(obj).__name__ == 'EnumTypeWrapper'

    def python_repr(self, obj):
      if isinstance(obj, message.Message):
        return self.message_repr(obj)
      elif isinstance(obj, (list,
                            pyext_message.RepeatedCompositeContainer,  # pylint: disable=c-extension-no-member
                            pyext_message.RepeatedScalarContainer)):  # pylint: disable=c-extension-no-member
        return '[%s]' % ', '.join(self.python_repr(x) for x in obj)
      else:
        return repr(obj)

    def empty_type(self, typ):
      name = ('EMPTY_' +
              '_'.join(x.upper()
                       for x in self.CAP_SPLIT.findall(typ.__name__)))
      self.empty_types.add('%s = %s()' % (name, self.import_type(typ)))
      return name

    def message_repr(self, msg):
      parts = []
      for field, value in msg.ListFields():
        parts.append('%s=%s' % (field.name, self.python_repr(value)))
      if parts:
        return '%s(%s)' % (self.import_type(type(msg)), ', '.join(parts))
      else:
        return self.empty_type(type(msg))

    def write_enum(self, enum_name, enum, indent):
      ctx = Context(indent=indent)

      with ctx.indent():
        for v in enum.DESCRIPTOR.values:
          extensions = v.GetOptions().Extensions

          prop = (
              extensions[beam_runner_api_pb2.beam_urn],
              extensions[beam_runner_api_pb2.beam_constant],
              extensions[metrics_pb2.monitoring_info_spec],
              extensions[metrics_pb2.label_props],
          )
          reprs = [self.python_repr(x) for x in prop]
          if all(x == "''" or x.startswith('EMPTY_') for x in reprs):
            continue
          ctx.line('%s = PropertiesFromEnumValue(%s)' %
                   (v.name, ', '.join(self.python_repr(x) for x in prop)))

      if ctx.lines:
        ctx.prepend('class %s(object):' % enum_name)
        ctx.prepend('')
        ctx.line('')
      return ctx.lines

    def write_message(self, message_name, message, indent=0):
      ctx = Context(indent=indent)

      with ctx.indent():
        for obj_name, obj in inspect.getmembers(message):
          if self.is_message_type(obj):
            ctx.lines += self.write_message(obj_name, obj, ctx._indent)
          elif self.is_enum_type(obj):
            ctx.lines += self.write_enum(obj_name, obj, ctx._indent)

      if ctx.lines:
        ctx.prepend('class %s(object):' % message_name)
        ctx.prepend('')
      return ctx.lines

  pb2_files = [path for path in glob.glob(os.path.join(out_dir, '*_pb2.py'))]
  api_path = os.path.dirname(pb2_files[0])
  sys.path.insert(0, os.path.dirname(api_path))

  def _import(m):
    # TODO: replace with importlib when we drop support for python2.
    return __import__('api.%s' % m, fromlist=[None])

  try:
    beam_runner_api_pb2 = _import('beam_runner_api_pb2')
    metrics_pb2 = _import('metrics_pb2')

    for pb2_file in pb2_files:
      modname = os.path.splitext(pb2_file)[0]
      out_file = modname + '_urns.py'
      modname = os.path.basename(modname)
      mod = _import(modname)

      ctx = Context()
      for obj_name, obj in inspect.getmembers(mod):
        if ctx.is_message_type(obj):
          ctx.lines += ctx.write_message(obj_name, obj)

      if ctx.lines:
        for line in reversed(sorted(ctx.empty_types)):
          ctx.prepend(line)

        for modname in reversed(sorted(ctx.imports)):
          ctx.prepend('from . import %s' % modname)

        ctx.prepend('from ..utils import PropertiesFromEnumValue')

        log.info("Writing urn stubs: %s" % out_file)
        with open(out_file, 'w') as f:
          f.writelines(ctx.lines)

  finally:
    sys.path.pop(0)


def generate_proto_files(force=False):

  py_sdk_root = os.path.dirname(os.path.abspath(__file__))
  proto_dirs = [os.path.join(py_sdk_root, path) for path in BEAM_PROTO_PATHS]
  proto_files = sum(
      [glob.glob(os.path.join(d, '*.proto')) for d in proto_dirs], [])
  out_dir = os.path.join(py_sdk_root, PYTHON_PROTO_OUTPUT_PATH)
  out_files = [path for path in glob.glob(os.path.join(out_dir, '*_pb2.py'))]

  if out_files and not proto_files and not force:
    # We have out_files but no protos; assume they're up to date.
    # This is actually the common case (e.g. installation from an sdist).
    log.info('No proto files; using existing generated files.')
    return

  elif not out_files and not proto_files:
    if not os.path.exists(py_sdk_root):
      raise RuntimeError(
          'Not in apache git tree; unable to find proto definitions.')
    else:
      raise RuntimeError(
          'No proto files found in %s.' % proto_dirs)

  if force:
    regenerate = 'forced'
  elif not out_files:
    regenerate = 'no output files'
  elif len(out_files) < len(proto_files):
    regenerate = 'not enough output files'
  elif (
      min(os.path.getmtime(path) for path in out_files)
      <= max(os.path.getmtime(path)
             for path in proto_files + [os.path.realpath(__file__)])):
    regenerate = 'output files are out-of-date'
  elif len(out_files) > len(proto_files):
    regenerate = 'output files without corresponding .proto files'
    # too many output files: probably due to switching between git branches.
    # remove them so they don't trigger constant regeneration.
    for out_file in out_files:
      os.remove(out_file)
  else:
    regenerate = None

  if regenerate:
    from grpc_tools import protoc

    log.info('Regenerating Python proto definitions (%s).' % regenerate)
    builtin_protos = resource_filename('grpc_tools', '_proto')
    args = (
        [sys.executable] +  # expecting to be called from command line
        ['--proto_path=%s' % builtin_protos] +
        ['--proto_path=%s' % d for d in proto_dirs] +
        ['--python_out=%s' % out_dir] +
        # TODO(robertwb): Remove the prefix once it's the default.
        ['--grpc_python_out=grpc_2_0:%s' % out_dir] +
        proto_files)
    ret_code = protoc.main(args)
    if ret_code:
      raise RuntimeError(
          'Protoc returned non-zero status (see logs for details): '
          '%s' % ret_code)

    # copy resource files
    for path in MODEL_RESOURCES:
      shutil.copy2(os.path.join(py_sdk_root, path), out_dir)

    ret_code = subprocess.call(
        ["futurize", "--both-stages", "--write", "--no-diff", out_dir])
    if ret_code:
      raise RuntimeError(
          'Error applying futurize to generated protobuf python files.')

    generate_urn_files(log, out_dir)

# We must generate protos after requirements from pyproject.toml are installed.
def generate_protos_first(original_cmd):

  class cmd(original_cmd, object):
    def run(self):
      generate_proto_files()
      super(cmd, self).run()
  return cmd


python_requires = '>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*'

if sys.version_info[0] == 2:
  warnings.warn(
      'You are using Apache Beam with Python 2. '
      'New releases of Apache Beam will soon support Python 3 only.')

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
        '*/*.pyx', '*/*/*.pyx', '*/*.pxd', '*/*/*.pxd', 'testing/data/*.yaml',
        'portability/api/*.yaml']},
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
    # BEAM-8840: Do NOT use tests_require or setup_requires.
    extras_require={
        'docs': ['Sphinx>=1.5.2,<2.0'],
        'test': REQUIRED_TEST_PACKAGES,
        'gcp': GCP_REQUIREMENTS,
        'interactive': INTERACTIVE_BEAM,
        'aws': AWS_REQUIREMENTS
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
        'sdist': generate_protos_first(sdist),
        'mypy': generate_protos_first(mypy),
    },
)
