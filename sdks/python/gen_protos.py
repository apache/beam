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

"""Generates Python proto modules and grpc stubs for Beam protos."""
from __future__ import absolute_import
from __future__ import print_function

import contextlib
import glob
import inspect
import logging
import multiprocessing
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import warnings

import pkg_resources

BEAM_PROTO_PATHS = [
    os.path.join('..', '..', 'model', 'pipeline', 'src', 'main', 'proto'),
    os.path.join('..', '..', 'model', 'job-management', 'src', 'main', 'proto'),
    os.path.join('..', '..', 'model', 'fn-execution', 'src', 'main', 'proto'),
    os.path.join('..', '..', 'model', 'interactive', 'src', 'main', 'proto'),
]

PYTHON_OUTPUT_PATH = os.path.join('apache_beam', 'portability', 'api')

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
      return fullpath
  raise RuntimeError("Could not find %s in %s" %
                     (fname, ', '.join(search_paths)))


def generate_proto_files(force=False, log=None):

  try:
    import grpc_tools  # pylint: disable=unused-import
  except ImportError:
    warnings.warn('Installing grpcio-tools is recommended for development.')

  if log is None:
    logging.basicConfig()
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

  py_sdk_root = os.path.dirname(os.path.abspath(__file__))
  common = os.path.join(py_sdk_root, '..', 'common')
  proto_dirs = [os.path.join(py_sdk_root, path) for path in BEAM_PROTO_PATHS]
  proto_files = sum(
      [glob.glob(os.path.join(d, '*.proto')) for d in proto_dirs], [])
  out_dir = os.path.join(py_sdk_root, PYTHON_OUTPUT_PATH)
  out_files = [path for path in glob.glob(os.path.join(out_dir, '*_pb2.py'))]

  if out_files and not proto_files and not force:
    # We have out_files but no protos; assume they're up to date.
    # This is actually the common case (e.g. installation from an sdist).
    log.info('No proto files; using existing generated files.')
    return

  elif not out_files and not proto_files:
    if not os.path.exists(common):
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
    try:
      from grpc_tools import protoc
    except ImportError:
      if platform.system() == 'Windows':
        # For Windows, grpcio-tools has to be installed manually.
        raise RuntimeError(
            'Cannot generate protos for Windows since grpcio-tools package is '
            'not installed. Please install this package manually '
            'using \'pip install grpcio-tools\'.')

      # Use a subprocess to avoid messing with this process' path and imports.
      # Note that this requires a separate module from setup.py for Windows:
      # https://docs.python.org/2/library/multiprocessing.html#windows
      p = multiprocessing.Process(
          target=_install_grpcio_tools_and_generate_proto_files,
          kwargs={'force': force})
      p.start()
      p.join()
      if p.exitcode:
        raise ValueError("Proto generation failed (see log for details).")
    else:
      log.info('Regenerating Python proto definitions (%s).' % regenerate)
      builtin_protos = pkg_resources.resource_filename('grpc_tools', '_proto')

      protoc_gen_mypy = _find_protoc_gen_mypy()

      log.info('Found protoc_gen_mypy at %s' % protoc_gen_mypy)

      args = (
          [sys.executable] +  # expecting to be called from command line
          ['--proto_path=%s' % builtin_protos] +
          ['--proto_path=%s' % d for d in proto_dirs] +
          ['--python_out=%s' % out_dir] +
          ['--plugin=protoc-gen-mypy=%s' % protoc_gen_mypy] +
          ['--mypy_out=%s' % out_dir] +
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

  else:
    log.info('Skipping proto regeneration: all files up to date')


# Though wheels are available for grpcio-tools, setup_requires uses
# easy_install which doesn't understand them.  This means that it is
# compiled from scratch (which is expensive as it compiles the full
# protoc compiler).  Instead, we attempt to install a wheel in a temporary
# directory and add it to the path as needed.
# See https://github.com/pypa/setuptools/issues/377
def _install_grpcio_tools_and_generate_proto_files(force=False):
  py_sdk_root = os.path.dirname(os.path.abspath(__file__))
  install_path = os.path.join(py_sdk_root, '.eggs', 'grpcio-wheels')
  build_path = install_path + '-build'
  if os.path.exists(build_path):
    shutil.rmtree(build_path)
  logging.warning('Installing grpcio-tools into %s', install_path)
  try:
    start = time.time()
    subprocess.check_call(
        [sys.executable, '-m', 'pip', 'install',
         '--target', install_path, '--build', build_path,
         '--upgrade',
         '-r', os.path.join(py_sdk_root, 'build-requirements.txt')])
    logging.warning(
        'Installing grpcio-tools took %0.2f seconds.', time.time() - start)
  finally:
    sys.stderr.flush()
    shutil.rmtree(build_path, ignore_errors=True)
  sys.path.append(install_path)
  try:
    generate_proto_files(force=force)
  finally:
    sys.stderr.flush()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  generate_proto_files(force=True)
