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
Upgrade the pip wheel bundled in ensurepip for Python 3.12+.

The script is executed within Docker after the image pip has been upgraded.
upgrade_ensurepip expects setuptools to be bundled as well, but Python 3.12+
only ships pip in ensurepip/_bundled.
"""

import subprocess
import sys
from pathlib import Path

import ensurepip


def main():
  ep_path = Path(ensurepip.__file__)
  wheel_dir = ep_path.parent / '_bundled'
  pip_version = subprocess.check_output(
      [sys.executable, '-m', 'pip', '--version'],
      text=True).split()[1]
  subprocess.check_call([
      sys.executable,
      '-m',
      'pip',
      'download',
      'pip=={}'.format(pip_version),
      '-d',
      str(wheel_dir),
      '--no-deps',
  ])
  lines = ep_path.read_text().splitlines()
  pip_line = None
  for idx, line in enumerate(lines):
    if line.startswith('_PIP_VERSION = '):
      pip_line = idx
      break
  if pip_line is None:
    sys.exit('ensurepip _PIP_VERSION not found')
  org = ep_path.with_suffix('.py.org')
  if not org.exists():
    ep_path.rename(org)
  lines[pip_line] = '_PIP_VERSION = "{}"'.format(pip_version)
  ep_path.write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
  main()
