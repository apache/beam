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
"""A script to pull licenses for Go.
"""

import os
import re
import shutil
import subprocess

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

try:
    from bs4 import BeautifulSoup
except:
    command = 'pip install bs4 --no-cache-dir'
    run_bash_command(command)
    from bs4 import BeautifulSoup


def get_final_dependencies(deps):
    final_deps = set()
    for dep in deps:
        # remove Beam internal dependencies.
        if dep.startswith('github.com/apache/beam'):
            continue
        # get original dependencies for vendorred dependencies..
        if dep.startswith('vendor/'):
            dep = dep.replace('vendor/', '')
        # the list contains all nested dependencies, following if statements
        # dedup nested dependencies and includes root dependencies only.
        # if dep is from github.com, ex: github.com/golang/protobuf/proto
        if dep.startswith('github.com'):
            final_deps.add('/'.join(dep.split('/')[:3]))
        # if dep is from google.golang.org, ex:google.golang.org/grpc
        elif dep.startswith('google.golang.org'):
            final_deps.add('/'.join(dep.split('/')[:2]))
        # if dep is from golang.org, ex: golang.org/x/net/http2
        elif dep.startswith('golang.org'):
            final_deps.add('/'.join(dep.split('/')[:3]))
        else:  # embedded dependencies, ex: debug, crypto/tls
            final_deps.add(dep.split('/')[0])
    return final_deps


def get_dependency_list():
    command = "go list -f '{{.Deps}}' github.com/apache/beam/sdks/go/pkg/beam"
    dependencies = run_bash_command(command)
    # dependencies returned from the command is '[dep0 dpe1 ...]'.
    # "'", "[", "]" should be removed from the bytes.
    str_dependencies = re.sub(r"([\'\[\]])", r"", dependencies)
    final_dependencies = get_final_dependencies(str_dependencies.split())
    return final_dependencies


@retry(stop=stop_after_attempt(3))
def pull_license(dep):
    '''
    :param dep: name of a dependency
    :return: boolean

    License files can be accessed at https://pkg.go.dev/{dep}.
    To pull license files, we are doing
    1. Download the HTML to a temp directory.
    2. Parse the HTML and read license context from the HTML
    3. Write the license context to the final license file.
    4. Remove the html file downloaded at step 1.
    '''
    url = 'https://pkg.go.dev/%s?tab=licenses' % dep
    dest_dir = '/'.join([license_dir, dep.replace('/', '_')])
    cur_temp_dir = 'temp_license_' + dep.replace('/', '_')
    os.mkdir(cur_temp_dir)
    if not os.path.isdir(dest_dir):
        os.mkdir(dest_dir)

    try:
        wget.download(url, cur_temp_dir + '/LICENSE.html')
        with open(cur_temp_dir + '/LICENSE.html') as f:
            html_context = f.read()
        soup = BeautifulSoup(html_context, 'html.parser')
        license_context = soup.section.pre.get_text()
        with open(dest_dir + '/LICENSE', 'w') as f:
            f.write(license_context)
        result = True
    except Exception as e:
        print('Error occurred when pull license from url.', 'dependency =',
              dep, 'url =', url, 'error = ', e.decode('utf-8'))
        result = False
    finally:
        shutil.rmtree(cur_temp_dir)
        return result


if __name__ == "__main__":
    cur_dir = os.getcwd()
    if cur_dir.split('/')[-1] != 'beam':
        raise RuntimeError('This script should be run from ~/beam directory.')
    license_dir = os.getcwd() + '/licenses/go'
    no_licenses = []
    dependencies = get_dependency_list()
    for dep in dependencies:
        if not pull_license(dep):
            no_licenses.append(dep)
    if no_licenses:
        raise RuntimeError('Some dependencies are missing license files. %s' %
                           no_licenses)
