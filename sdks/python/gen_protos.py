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

"""
Generates Python proto modules and grpc stubs for Beam protos.
"""

import contextlib
import glob
import inspect
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from importlib import import_module

import pkg_resources

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

LICENSE_HEADER = """
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
"""

NO_PROMISES_NOTICE = """
\"\"\"
For internal use only; no backwards-compatibility guarantees.
Automatically generated when running setup.py sdist or build[_py].
\"\"\"
"""


def clean_path(path):
  return os.path.realpath(os.path.abspath(path))


# These paths are relative to the project root
BEAM_PROTO_PATHS = [
    os.path.join('model', 'pipeline', 'src', 'main', 'proto'),
    os.path.join('model', 'job-management', 'src', 'main', 'proto'),
    os.path.join('model', 'fn-execution', 'src', 'main', 'proto'),
    os.path.join('model', 'interactive', 'src', 'main', 'proto'),
]

PYTHON_SDK_ROOT = os.path.dirname(clean_path(__file__))
PROJECT_ROOT = clean_path(os.path.join(PYTHON_SDK_ROOT, '..', '..'))
PYTHON_OUTPUT_PATH = os.path.join(
    PYTHON_SDK_ROOT, 'apache_beam', 'portability', 'api')

MODEL_RESOURCES = [
    os.path.normpath((
        'model/fn-execution/src/main/resources/org/'
        'apache/beam/model/fnexecution/v1/standard_coders.yaml')),
]


class PythonPath(object):
  def __init__(self, path: str, front: bool = False):
    self._path = path
    self._front = front

  def __enter__(self):
    if not self._path:
      return

    self._sys_path = sys.path.copy()
    if self._front:
      sys.path.insert(0, self._path)
    else:
      sys.path.append(self._path)

  def __exit__(self, exc_type, exc_val, exc_tb):
    if not self._path:
      return

    sys.path = self._sys_path


def generate_urn_files(out_dir, api_path):
  """
  Create python files with statically defined URN constants.

  Creates a <proto>_pb2_urn.py file for each <proto>_pb2.py file that contains
  an enum type.

  This works by importing each api.<proto>_pb2 module created by `protoc`,
  inspecting the module's contents, and generating a new side-car urn module.
  This is executed at build time rather than dynamically on import to ensure
  that it is compatible with static type checkers like mypy.
  """
  from google.protobuf import message
  from google.protobuf.internal import api_implementation
  if api_implementation.Type() == 'python':
    from google.protobuf.internal import containers
    repeated_types = (
        list,
        containers.RepeatedScalarFieldContainer,
        containers.RepeatedCompositeFieldContainer)
  elif api_implementation.Type() == 'upb':
    from google._upb import _message
    repeated_types = (
        list,
        _message.RepeatedScalarContainer,
        _message.RepeatedCompositeContainer)
  elif api_implementation.Type() == 'cpp':
    from google.protobuf.pyext import _message
    repeated_types = (
        list,
        _message.RepeatedScalarContainer,
        _message.RepeatedCompositeContainer)
  else:
    raise TypeError(
        "Unknown proto implementation: " + api_implementation.Type())

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
        _, modname = modname.rsplit('.', 1)
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
      elif isinstance(obj, repeated_types):
        return '[%s]' % ', '.join(self.python_repr(x) for x in obj)
      else:
        return repr(obj)

    def empty_type(self, typ):
      name = (
          'EMPTY_' +
          '_'.join(x.upper() for x in self.CAP_SPLIT.findall(typ.__name__)))
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
        for enum_value_name in enum.values_by_name:
          enum_value_descriptor = enum.values_by_name[enum_value_name]
          extensions = enum_value_descriptor.GetOptions().Extensions
          prop = (
              extensions[beam_runner_api_pb2.beam_urn],
              extensions[beam_runner_api_pb2.beam_constant],
              extensions[metrics_pb2.monitoring_info_spec],
              extensions[metrics_pb2.label_props],
          )
          reprs = [self.python_repr(x) for x in prop]
          if all(x == "''" or x.startswith('EMPTY_') for x in reprs):
            continue
          ctx.line(
              '%s = PropertiesFromEnumValue(%s)' %
              (enum_value_name, ', '.join(self.python_repr(x) for x in prop)))

      if ctx.lines:
        ctx.prepend('class %s(object):' % enum_name)
        ctx.prepend('')
        ctx.line('')
      return ctx.lines

    def write_message(self, message_name, message, indent=0):
      ctx = Context(indent=indent)

      with ctx.indent():
        for obj_name, obj in inspect.getmembers(message):
          if obj_name == 'DESCRIPTOR':
            for enum_name in obj.enum_types_by_name:
              enum = obj.enum_types_by_name[enum_name]
              ctx.lines += self.write_enum(enum_name, enum, ctx._indent)

      if ctx.lines:
        ctx.prepend('class %s(object):' % message_name)
        ctx.prepend('')
      return ctx.lines

  pb2_files = list(glob.glob(os.path.join(out_dir, '*_pb2.py')))

  with PythonPath(os.path.dirname(api_path), front=True):
    beam_runner_api_pb2 = import_module(
        'api.org.apache.beam.model.pipeline.v1.beam_runner_api_pb2')
    metrics_pb2 = import_module(
        'api.org.apache.beam.model.pipeline.v1.metrics_pb2')

    for pb2_file in pb2_files:
      modname = os.path.splitext(pb2_file)[0]
      out_file = modname + '_urns.py'
      api_start_idx = modname.index(os.path.sep + 'api' + os.path.sep)
      import_path = modname[api_start_idx + 1:].replace(os.path.sep, '.')
      mod = import_module(import_path)

      ctx = Context()
      for obj_name, obj in inspect.getmembers(mod):
        if ctx.is_message_type(obj):
          ctx.lines += ctx.write_message(obj_name, obj)

      if ctx.lines:
        for line in reversed(sorted(ctx.empty_types)):
          ctx.prepend(line)

        for modname in reversed(sorted(ctx.imports)):
          pkg, target = modname.rsplit('.', 1)
          rel_import = build_relative_import(api_path, pkg, out_file)
          ctx.prepend('from %s import %s' % (rel_import, target))

        rel_import = build_relative_import(
            os.path.dirname(api_path), 'utils', out_file)
        ctx.prepend('from %s import PropertiesFromEnumValue' % rel_import)

        LOG.info("Writing urn stubs: %s" % out_file)
        with open(out_file, 'w') as f:
          f.writelines(ctx.lines)


def _find_protoc_gen_mypy():
  # NOTE: this shouldn't be necessary if the virtualenv's environment
  #  is passed to tasks below it, since protoc will search the PATH itself
  fname = 'protoc-gen-mypy'
  if platform.system() == 'Windows':
    fname += ".exe"

  pathstr = os.environ.get('PATH')
  search_paths = pathstr.split(os.pathsep) if pathstr else []
  # should typically be installed into the venv's bin dir
  search_paths.insert(0, os.path.dirname(sys.executable))
  for path in search_paths:
    fullpath = os.path.join(path, fname)
    if os.path.exists(fullpath):
      LOG.info('Found protoc_gen_mypy at %s' % fullpath)
      return fullpath
  raise RuntimeError(
      "Could not find %s in %s" % (fname, ', '.join(search_paths)))


def find_by_ext(root_dir, ext):
  for root, _, files in os.walk(root_dir):
    for file in files:
      if file.endswith(ext):
        yield clean_path(os.path.join(root, file))


def ensure_grpcio_exists():
  try:
    from grpc_tools import protoc  # pylint: disable=unused-import
  except ImportError:
    return _install_grpcio_tools()


def _install_grpcio_tools():
  """
  Though wheels are available for grpcio-tools, setup_requires uses
  easy_install which doesn't understand them.  This means that it is
  compiled from scratch (which is expensive as it compiles the full
  protoc compiler).  Instead, we attempt to install a wheel in a temporary
  directory and add it to the path as needed.
  See https://github.com/pypa/setuptools/issues/377
  """
  install_path = os.path.join(PYTHON_SDK_ROOT, '.eggs', 'grpcio-wheels')
  logging.warning('Installing grpcio-tools into %s', install_path)
  start = time.time()
  subprocess.check_call([
      sys.executable,
      '-m',
      'pip',
      'install',
      '--target',
      install_path,
      '--upgrade',
      '-r',
      os.path.join(PYTHON_SDK_ROOT, 'build-requirements.txt')
  ])
  logging.warning(
      'Installing grpcio-tools took %0.2f seconds.', time.time() - start)

  return install_path


def build_relative_import(root_path, import_path, start_file_path):
  tail_path = import_path.replace('.', os.path.sep)
  source_path = os.path.join(root_path, tail_path)

  is_module = os.path.isfile(source_path + '.py')
  if is_module:
    source_path = os.path.dirname(source_path)

  rel_path = os.path.relpath(
      source_path, start=os.path.dirname(start_file_path))

  if rel_path == '.':
    if is_module:
      rel_path += os.path.basename(tail_path)

    return rel_path

  if rel_path.endswith('..'):
    rel_path += os.path.sep

  # In a path that looks like ../../../foo, every double dot
  # after the right most double dot needs to be collapsed to
  # a single dot to look like ././../foo to which we can convert
  # to ....foo for the proper relative import.
  first_half_idx = rel_path.rfind('..' + os.path.sep)
  if first_half_idx == 0:
    return rel_path.replace(os.path.sep, '')

  first_half = rel_path[:first_half_idx].replace('..', '.')
  final_import = first_half.replace(os.path.sep, '') + '..' + \
         rel_path[first_half_idx+3:].replace(os.path.sep, '.')

  if is_module:
    if final_import.count('.') == len(final_import):
      return final_import + os.path.basename(tail_path)

    return final_import + '.{}'.format(os.path.basename(tail_path))

  return final_import


def generate_init_files_lite(api_root):
  proto_root = os.path.join(api_root, 'org')
  for root, _, _ in os.walk(proto_root):
    init_file = os.path.join(root, '__init__.py')
    with open(init_file, 'w+'):
      pass


def generate_init_files_full(api_root):
  proto_root = os.path.join(api_root, 'org')
  api_module_root = os.path.join(api_root, '__init__.py')
  modules = defaultdict(list)

  for root, _, files in os.walk(proto_root):
    init_file = os.path.join(root, '__init__.py')
    with open(init_file, 'w+') as f:
      f.write(LICENSE_HEADER.lstrip())
      for file in files:
        if not file.endswith('.py') or file == '__init__.py':
          continue
        module_name = file.split('.')[0]
        f.write('from . import {}\n'.format(module_name))
        modules[root].append(module_name)

  with open(api_module_root, 'w+') as f:
    f.write(LICENSE_HEADER.lstrip())
    f.write(NO_PROMISES_NOTICE.lstrip())
    remaining_lines = []

    duplicate_modules = {}
    for module_root, modules in modules.items():
      import_path = os.path.relpath(module_root,
                                    api_root).replace(os.path.sep, '.')
      import_root, imported_module = import_path.rsplit('.', 1)

      if imported_module not in duplicate_modules:
        f.write('from .{} import {}\n'.format(import_root, imported_module))
        duplicate_modules[imported_module] = 1
      else:
        duplicate_modules[imported_module] += 1
        module_alias = '{}_{}'.format(
            imported_module, duplicate_modules[imported_module])
        f.write(
            'from .{} import {} as {}\n'.format(
                import_root, imported_module, module_alias))
        imported_module = module_alias

      for module in modules:
        remaining_lines.append(
            '{module} = {}.{module}\n'.format(imported_module, module=module))
    f.write('\n')
    f.writelines(remaining_lines)


def generate_proto_files(force=False):
  """
  Will compile proto files for python. If force is not true, then several
  heuristics are used to determine whether a compilation is necessary. If
  a compilation is not necessary, no compilation will be performed.
  :param force: Whether to force a recompilation of the proto files.
  """
  proto_dirs = [
      clean_path(os.path.join(PROJECT_ROOT, path)) for path in BEAM_PROTO_PATHS
  ]
  proto_files = [
      proto_file for d in proto_dirs for proto_file in find_by_ext(d, '.proto')
  ]

  out_files = list(find_by_ext(PYTHON_OUTPUT_PATH, '_pb2.py'))

  if out_files and not proto_files and not force:
    # We have out_files but no protos; assume they're up-to-date.
    # This is actually the common case (e.g. installation from an sdist).
    LOG.info('No proto files; using existing generated files.')
    return

  elif not out_files and not proto_files:
    model = os.path.join(PROJECT_ROOT, 'model')
    if os.path.exists(model):
      error_msg = 'No proto files found in %s.' % proto_dirs
    else:
      error_msg = 'Not in apache git tree, unable to find proto definitions.'

    raise RuntimeError(error_msg)

  if force:
    regenerate_reason = 'forced'
  elif not out_files:
    regenerate_reason = 'no output files'
  elif len(out_files) < len(proto_files):
    regenerate_reason = 'not enough output files'
  elif (min(os.path.getmtime(path) for path in out_files) <= max(
      os.path.getmtime(path)
      for path in proto_files + [os.path.realpath(__file__)])):
    regenerate_reason = 'output files are out-of-date'
  elif len(out_files) > len(proto_files):
    regenerate_reason = 'output files without corresponding .proto files'
    # too many output files: probably due to switching between git branches.
    # remove them so they don't trigger constant regeneration.
    for out_file in out_files:
      os.remove(out_file)
  else:
    regenerate_reason = ''

  if not regenerate_reason:
    LOG.info('Skipping proto regeneration: all files up to date')
    return

  shutil.rmtree(PYTHON_OUTPUT_PATH, ignore_errors=True)
  if not os.path.exists(PYTHON_OUTPUT_PATH):
    os.mkdir(PYTHON_OUTPUT_PATH)

  grpcio_install_loc = ensure_grpcio_exists()
  protoc_gen_mypy = _find_protoc_gen_mypy()
  with PythonPath(grpcio_install_loc):
    from grpc_tools import protoc
    builtin_protos = pkg_resources.resource_filename('grpc_tools', '_proto')
    args = (
        [sys.executable] +  # expecting to be called from command line
        ['--proto_path=%s' % builtin_protos] +
        ['--proto_path=%s' % d
         for d in proto_dirs] + ['--python_out=%s' % PYTHON_OUTPUT_PATH] +
        ['--plugin=protoc-gen-mypy=%s' % protoc_gen_mypy] +
        # new version of mypy-protobuf converts None to zero default value
        # and remove Optional from the param type annotation. This causes
        # some mypy errors. So to mitigate and fall back to old behavior,
        # use `relax_strict_optional_primitives` flag. more at
        # https://github.com/nipunn1313/mypy-protobuf/tree/main#relax_strict_optional_primitives # pylint:disable=line-too-long
        ['--mypy_out=relax_strict_optional_primitives:%s' % PYTHON_OUTPUT_PATH
         ] +
        # TODO(robertwb): Remove the prefix once it's the default.
        ['--grpc_python_out=grpc_2_0:%s' % PYTHON_OUTPUT_PATH] + proto_files)

    LOG.info('Regenerating Python proto definitions (%s).' % regenerate_reason)
    ret_code = protoc.main(args)
    if ret_code:
      raise RuntimeError(
          'Protoc returned non-zero status (see logs for details): '
          '%s' % ret_code)

  # copy resource files
  for path in MODEL_RESOURCES:
    shutil.copy2(os.path.join(PROJECT_ROOT, path), PYTHON_OUTPUT_PATH)

  proto_packages = set()
  # see: https://github.com/protocolbuffers/protobuf/issues/1491
  # force relative import paths for proto files
  compiled_import_re = re.compile('^from (.*) import (.*)$')
  for file_path in find_by_ext(PYTHON_OUTPUT_PATH,
                               ('_pb2.py', '_pb2_grpc.py', '_pb2.pyi')):
    proto_packages.add(os.path.dirname(file_path))
    lines = []
    with open(file_path, encoding='utf-8') as f:
      for line in f:
        match_obj = compiled_import_re.match(line)
        if match_obj and \
                match_obj.group(1).startswith('org.apache.beam.model'):
          new_import = build_relative_import(
              PYTHON_OUTPUT_PATH, match_obj.group(1), file_path)
          line = 'from %s import %s\n' % (new_import, match_obj.group(2))

        lines.append(line)

    with open(file_path, 'w') as f:
      f.writelines(lines)

  generate_init_files_lite(PYTHON_OUTPUT_PATH)
  with PythonPath(grpcio_install_loc):
    for proto_package in proto_packages:
      generate_urn_files(proto_package, PYTHON_OUTPUT_PATH)

    generate_init_files_full(PYTHON_OUTPUT_PATH)


if __name__ == '__main__':
  generate_proto_files(force=True)
