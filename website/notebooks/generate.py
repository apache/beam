#!/usr/bin/env python
#
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

# This script generates notebooks for all the files defined in `docs.yaml`.
# It has to be run manually if the docs or code snippets are updated.
#
# To run, you have to install `md2ipynb`.
#   pip install -U md2ipynb
#
# Then it can be run without any arguments.
#   python website/notebooks/generate.py
#
# This creates the output notebooks in the `examples/notebooks` directory.
# You have to commit the generated notebooks after generating them.

import logging
import md2ipynb
import nbformat
import os
import sys
import yaml

docs_logo_url = 'https://beam.apache.org/images/logos/full-color/name-bottom/beam-logo-full-color-name-bottom-100.png'


def run(docs, root_dir, variables=None,
        inputs_dir='.', outputs_dir='.', imports_dir='.', include_dir='.'):

  errors = []
  for basename, doc in docs.items():
    languages=doc.get('languages', 'py java go').split()
    for lang in languages:
      # Read the imports defined in the docs.yaml.
      imports = {
          i: [os.path.join(imports_dir, path) for path in imports]
          for i, imports in doc.get('imports', {}).items()
      }

      # Make sure the first import in section 0 is the license.md.
      if 0 not in imports:
        imports[0] = []
      imports[0].insert(0, os.path.join(imports_dir, 'license.md'))

      # Create a new notebook from the Markdown file contents.
      input_file = basename + '.md'
      ipynb_file = '/'.join([outputs_dir, '{}-{}.ipynb'.format(basename, lang)])
      try:
        notebook = md2ipynb.new_notebook(
            input_file=os.path.join(inputs_dir, input_file),
            variables=variables,
            imports=imports,
            notebook_title=doc.get('title', os.path.basename(basename).replace('-', ' ')),
            keep_classes=['language-' + lang, 'shell-sh'],
            filter_classes='notebook-skip',
            docs_url='https://beam.apache.org/' + basename.replace('-', ''),
            docs_logo_url=docs_logo_url,
            github_ipynb_url='https://github.com/apache/beam/blob/master/'
                + os.path.relpath(ipynb_file, root_dir),
            include_dir=include_dir,
        )
        logging.info('{} succeeded'.format(input_file))

        # Write the notebook to file.
        output_dir = os.path.dirname(ipynb_file)
        if not os.path.exists(output_dir):
          os.makedirs(output_dir)
        with open(ipynb_file, 'w') as f:
          nbformat.write(notebook, f)
      except Exception as e:
        logging.error('{} failed: {}'.format(input_file, e))
        errors.append((input_file, e))

  if errors:
    import traceback
    sys.stdout.flush()
    sys.stderr.flush()
    print('')
    print('=' * 60)
    print(' Errors')
    for input_file, e in errors:
      print('')
      print(input_file)
      print('-' * len(input_file))
      traceback.print_exception(type(e), e, e.__traceback__)

  print('')
  print('{} files processed ({} succeeded, {} failed)'.format(
    len(docs), len(docs) - len(errors), len(errors)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  script_dir = os.path.dirname(os.path.realpath(__file__))
  root_dir = os.path.realpath(os.path.join(script_dir, '..', '..'))

  docs_file = os.path.join(script_dir, 'docs.yaml')
  with open(docs_file) as f:
    docs = yaml.load(f.read())

  variables_file = os.path.join(root_dir, 'website', '_config.yml')
  with open(variables_file) as f:
    variables = {'site': yaml.load(f.read())}
    variables['site']['baseurl'] = variables['site']['url']

  run(
      docs=docs,
      root_dir=root_dir,
      variables=variables,
      inputs_dir=os.path.join(root_dir, 'website', 'src'),
      outputs_dir=os.path.join(root_dir, 'examples', 'notebooks'),
      imports_dir=os.path.join(script_dir, 'imports'),
      include_dir=os.path.join(root_dir, 'website', 'src', '_includes'),
  )
