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
"""A script to pull licenses for Python.
"""
import json
import os
import shutil
import subprocess
import sys
import yaml

from tenacity import retry
from tenacity import stop_after_attempt


def run_bash_command(command):
    process = subprocess.Popen(command.split(),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    result, error = process.communicate()
    if error:
        raise RuntimeError('Error occurred when running a bash command.',
                           'command: ', command, 'error message: ',
                           error.decode('utf-8'))
    return result.decode('utf-8')


try:
    import wget
except:
    command = 'pip install wget --no-cache-dir'
    run_bash_command(command)
    import wget


def install_pip_licenses():
    command = 'pip install pip-licenses --no-cache-dir'
    run_bash_command(command)


def run_pip_licenses():
    command = 'pip-licenses --with-license-file --format=json'
    dependencies = run_bash_command(command)
    return json.loads(dependencies)


@retry(stop=stop_after_attempt(3))
def copy_license_files(dep):
    source_license_file = dep['LicenseFile']
    if source_license_file.lower() == 'unknown':
        return False
    name = dep['Name']
    dest_dir = '/'.join([license_dir, name.lower()])
    try:
        if not os.path.isdir(dest_dir):
            os.mkdir(dest_dir)
        shutil.copy(source_license_file, dest_dir + '/LICENSE')
        return True
    except Exception as e:
        print(e)
        return False


@retry(stop=stop_after_attempt(3))
def pull_from_url(dep, configs):
    '''
    :param dep: name of a dependency
    :param configs: a dict from dep_urls_py.yaml
    :return: boolean

    It downloads files form urls to a temp directory first in order to avoid
    to deal with any temp files. It helps keep clean final directory.
    '''
    if dep in configs.keys():
        config = configs[dep]
        dest_dir = '/'.join([license_dir, dep])
        cur_temp_dir = 'temp_license_' + dep
        os.mkdir(cur_temp_dir)
        try:
            is_file_available = False
            # license is required, but not all dependencies have license.
            # In case we have to skip, print out a message.
            if config['license'] != 'skip':
                wget.download(config['license'], cur_temp_dir + '/LICENSE')
                is_file_available = True
            # notice is optional.
            if 'notice' in config:
                wget.download(config['notice'], cur_temp_dir + '/NOTICE')
                is_file_available = True
            # copy from temp dir to final dir only when either file is abailable.
            if is_file_available:
                if os.path.isdir(dest_dir):
                    shutil.rmtree(dest_dir)
                shutil.copytree(cur_temp_dir, dest_dir)
            result = True
        except Exception as e:
            print('Error occurred when pull license from url.', 'dependency =',
                  dep, 'url =', config, 'error = ', e.decode('utf-8'))
            result = False
        finally:
            shutil.rmtree(cur_temp_dir)
            return result
    else:
        return False


if __name__ == "__main__":
    cur_dir = os.getcwd()
    if cur_dir.split('/')[-1] != 'beam':
        raise RuntimeError('This script should be run from ~/beam directory.')
    license_dir = os.getcwd() + '/licenses/python'
    no_licenses = []

    with open('license-scripts/dep_urls_py.yaml') as file:
        dep_config = yaml.load(file)

    install_pip_licenses()
    dependencies = run_pip_licenses()
    # add licenses for pip installed packages.
    # try to pull licenses with pip-licenses tool first, if no license pulled,
    # then pull from URLs.
    for dep in dependencies:
        if not (copy_license_files(dep) or pull_from_url(
                dep['Name'].lower(), dep_config['pip_dependencies'])):
            no_licenses.append(dep['Name'])

    # add licenses for dependencies added to docker
    for dep in dep_config['docker_dependencies']:
        if not pull_from_url(dep, dep_config['docker_dependencies']):
            no_licenses.append(dep)

    # check if license are already pulled.
    no_licenses = [
        dep for dep in no_licenses
        if not os.path.isdir(license_dir + '/' + dep.lower())
    ]

    if no_licenses:
        py_version = '%d.%d' % (sys.version_info[0], sys.version_info[1])
        raise RuntimeError(
            'Some dependencies are missing license files at python{py_version} '
            'environment. {license_list}'.format(py_version=py_version,
                                                 license_list=no_licenses))
