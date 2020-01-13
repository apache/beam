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

import glob
import logging
import multiprocessing
import os
import platform
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


def generate_proto_files(force=False, log=None):

  try:
    import grpc_tools  # pylint: disable=unused-import
  except ImportError:
    warnings.warn('Installing grpcio-tools is recommended for development.')

  if log is None:
    log = logging.getLogger(__name__)

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
          target=_install_grpcio_tools_and_generate_proto_files)
      p.start()
      p.join()
      if p.exitcode:
        raise ValueError("Proto generation failed (see log for details).")
    else:
      log.info('Regenerating Python proto definitions (%s).' % regenerate)
      builtin_protos = pkg_resources.resource_filename('grpc_tools', '_proto')
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

      ret_code = subprocess.call(["pip", "install", "future==0.16.0"])
      if ret_code:
        raise RuntimeError(
            'Error installing future during proto generation')

      ret_code = subprocess.call(
          ["futurize", "--both-stages", "--write", "--no-diff", out_dir])
      if ret_code:
        raise RuntimeError(
            'Error applying futurize to generated protobuf python files.')


# Though wheels are available for grpcio-tools, setup_requires uses
# easy_install which doesn't understand them.  This means that it is
# compiled from scratch (which is expensive as it compiles the full
# protoc compiler).  Instead, we attempt to install a wheel in a temporary
# directory and add it to the path as needed.
# See https://github.com/pypa/setuptools/issues/377
def _install_grpcio_tools_and_generate_proto_files():
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
    generate_proto_files()
  finally:
    sys.stderr.flush()


if __name__ == '__main__':
  generate_proto_files(force=True)
