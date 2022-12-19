# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Script to recursively patch Colab notebooks.
Replaces links to point to the master branch and adds a license.

Usage:
  From root directory, execute
  'python examples/notebooks/patch.py'.
"""
import argparse
import collections
import json
import os
import re

license_text = """\
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
""".splitlines()

colab_url = 'https://colab.research.google.com/github'
github_url = 'https://github.com'
branch_repo_re = re.compile(r'[\w-]+/[\w-]+/blob/[\w-]+')
target_branch_repo = 'apache/beam/blob/master'


def run(root_dir):
  for path, dirs, files in os.walk(root_dir):
    for filename in files:
      if filename.endswith('.ipynb'):
        patch_notebook(os.path.join(path, filename))


def patch_notebook(full_path):
  # Replace the branch repo URL.
  lines = []
  with open(full_path) as f:
    for line in f:
      if colab_url in line or github_url in line:
        line = branch_repo_re.sub(target_branch_repo, line)
      lines += [line]
  content = ''.join(lines)

  # Add the license block.
  decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)
  elements = decoder.decode(content).items()
  first_key, _ = elements[0]
  if first_key != 'license':
    print('Patching {}'.format(full_path))
    elements.insert(0, ('license', license_text))

  # Overwrite the file with the new contents.
  with open(full_path, 'w') as f:
    json.dump(collections.OrderedDict(elements), f, indent=2)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--root-dir',
      type=str,
      default='examples/notebooks',
      help='Root directory to recursively patch *.ipynb notebooks.',
  )
  args = parser.parse_args()

  run(args.root_dir)
