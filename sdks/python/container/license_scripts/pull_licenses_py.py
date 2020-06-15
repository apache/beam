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
A script to pull licenses for Python.
The script is executed within Docker.
"""
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import yaml

from future.moves.urllib.request import urlopen
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

LICENSE_DIR = '/opt/apache/beam/third_party_licenses'


def run_bash_command(command):
  return subprocess.check_output(command.split()).decode('utf-8')


def run_pip_licenses():
  command = 'pip-licenses --with-license-file --format=json'
  dependencies = run_bash_command(command)
  return json.loads(dependencies)


@retry(stop=stop_after_attempt(3))
def copy_license_files(dep):
  source_license_file = dep['LicenseFile']
  if source_license_file.lower() == 'unknown':
    return False
  name = dep['Name'].lower()
  dest_dir = '/'.join([LICENSE_DIR, name])
  try:
    os.mkdir(dest_dir)
    shutil.copy(source_license_file, dest_dir + '/LICENSE')
    logging.debug(
        'Successfully pulled license for {dep} with pip-licenses.'.format(
            dep=name))
    return True
  except Exception as e:
    logging.error(
        'Failed to copy from {source} to {dest}'.format(
            source=source_license_file, dest=dest_dir + '/LICENSE'))
    traceback.print_exc()
    raise


@retry(
    reraise=True,
    wait=wait_exponential(multiplier=2),
    stop=stop_after_attempt(5))
def pull_from_url(dep, configs):
  '''
  :param dep: name of a dependency
  :param configs: a dict from dep_urls_py.yaml
  :return: boolean

  It downloads files form urls to a temp directory first in order to avoid
  to deal with any temp files. It helps keep clean final directory.
  '''
  if dep in configs:
    config = configs[dep]
    dest_dir = '/'.join([LICENSE_DIR, dep])
    cur_temp_dir = tempfile.mkdtemp()

    try:
      if config['license'] == 'skip':
        print('Skip pulling license for ', dep)
      else:
        url_read = urlopen(config['license'])
        with open(cur_temp_dir + '/LICENSE', 'wb') as temp_write:
          shutil.copyfileobj(url_read, temp_write)
        logging.debug(
            'Successfully pulled license for {dep} from {url}.'.format(
                dep=dep, url=config['license']))

      # notice is optional.
      if 'notice' in config:
        url_read = urlopen(config['notice'])
        with open(cur_temp_dir + '/NOTICE', 'wb') as temp_write:
          shutil.copyfileobj(url_read, temp_write)

      shutil.copytree(cur_temp_dir, dest_dir)
      return True
    except Exception as e:
      logging.error(
          'Error occurred when pull license for {dep} from {url}.'.format(
              dep=dep, url=config))
      traceback.print_exc()
      raise
    finally:
      shutil.rmtree(cur_temp_dir)


if __name__ == "__main__":
  os.makedirs(LICENSE_DIR)
  no_licenses = []
  logging.getLogger().setLevel(logging.INFO)

  with open('/tmp/license_scripts/dep_urls_py.yaml') as file:
    dep_config = yaml.full_load(file)

  dependencies = run_pip_licenses()
  # add licenses for pip installed packages.
  # try to pull licenses with pip-licenses tool first, if no license pulled,
  # then pull from URLs.
  for dep in dependencies:
    if not (copy_license_files(dep) or
            pull_from_url(dep['Name'].lower(), dep_config['pip_dependencies'])):
      no_licenses.append(dep['Name'].lower())

  if no_licenses:
    py_ver = '%d.%d' % (sys.version_info[0], sys.version_info[1])
    how_to = 'These licenses were not able to be pulled automatically. ' \
             'Please search code source of the dependencies on the internet ' \
             'and add urls to RAW license file at sdks/python/container/' \
             'license_scripts/dep_urls_py.yaml for each missing license ' \
             'and rerun the test. If no such urls can be found, you need ' \
             'to manually add LICENSE and NOTICE (if available) files at ' \
             'sdks/python/container/license_scripts/manual_licenses/{dep}/ ' \
             'and add entries to sdks/python/container/license_scripts/' \
             'dep_urls_py.yaml.'
    raise RuntimeError(
        'Could not retrieve licences for packages {license_list} in '
        'Python{py_ver} environment. \n {how_to}'.format(
            py_ver=py_ver,
            license_list=sorted(no_licenses),
            how_to=how_to))
  else:
    logging.info(
        'Successfully pulled licenses for {n} dependencies'.format(
            n=len(dependencies)))
