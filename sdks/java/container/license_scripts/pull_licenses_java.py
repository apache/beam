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
A script to pull licenses/notices/source code for Java dependencies.
It generates a CSV file with [dependency_name, url_to_license, license_type, source_included]
"""

import argparse
import csv
import json
import logging
import os
import shutil
import threading
import traceback
import yaml

from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing.pool import ThreadPool
from queue import Queue
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_fixed
from urllib.request import urlopen, Request, URLError, HTTPError

SOURCE_CODE_REQUIRED_LICENSES = ['lgpl', 'gpl', 'cddl', 'mpl', 'gnu', 'mozilla public license']
RETRY_NUM = 9
THREADS = 16

# workaround of a breaking change introduced in tenacity 8.5+ 
# See https://github.com/jd/tenacity/issues/486
def resolve_retry_number(retried_fn):
    return retried_fn.retry.statistics.get("attempt_number") or \
        retried_fn.statistics.get("attempt_number")

@retry(reraise=True,
       wait=wait_fixed(5),
       stop=stop_after_attempt(RETRY_NUM))
def pull_from_url(file_name, url, dep, no_list):
    if url == 'skip':
        return

    # Replace file path with absolute path to manual licenses
    if url.startswith('file://{}'):
        url = url.format(manual_license_path)
        logging.info('Replaced local file URL with {url} for {dep}'.format(url=url, dep=dep))

    # Take into account opensource.org changes that cause 404 on licenses
    if 'opensource.org' in url and url.endswith('-license.php'):
        url = url.replace('-license.php', '')

    try:
        url_read = urlopen(Request(url, headers={
            'User-Agent': 'Apache Beam',
            # MPL license fails to resolve redirects without this header
            # see https://github.com/apache/beam/issues/22394
            'accept-language': 'en-US,en;q=0.9',
        }))
        with open(file_name, 'wb') as temp_write:
            shutil.copyfileobj(url_read, temp_write)
        logging.debug(
            'Successfully pulled {file_name} from {url} for {dep}'.format(
                url=url, file_name=file_name, dep=dep))
    except URLError as e:
        traceback.print_exc()
        if resolve_retry_number(pull_from_url) < RETRY_NUM:
            logging.error('Invalid url for {dep}: {url}. Retrying...'.format(
                url=url, dep=dep))
            raise
        else:
            logging.error(
                'Invalid url for {dep}: {url} after {n} retries.'.format(
                    url=url, dep=dep, n=RETRY_NUM))
            with thread_lock:
                no_list.append(dep)
            return
    except HTTPError as e:
        traceback.print_exc()
        if resolve_retry_number(pull_from_url) < RETRY_NUM:
            logging.info(
                'Received {code} from {url} for {dep}. Retrying...'.format(
                    code=e.code, url=url, dep=dep))
            raise
        else:
            logging.error(
                'Received {code} from {url} for {dep} after {n} retries.'.
                format(code=e.code, url=url, dep=dep, n=RETRY_NUM))
            with thread_lock:
                no_list.append(dep)
            return
    except Exception as e:
        traceback.print_exc()
        if resolve_retry_number(pull_from_url) < RETRY_NUM:
            logging.error(
                'Error occurred when pull {file_name} from {url} for {dep}. Retrying...'
                .format(url=url, file_name=file_name, dep=dep))
            raise
        else:
            logging.error(
                'Error occurred when pull {file_name} from {url} for {dep} after {n} retries.'
                .format(url=url, file_name=file_name, dep=dep, n=RETRY_NUM))
            with thread_lock:
                no_list.append(dep)
            return


def pull_source_code(base_url, dir_name, dep):
    # base_url example: https://repo1.maven.org/maven2/org/mortbay/jetty/jsp-2.1/6.1.14/
    try:
      soup = BeautifulSoup(urlopen(base_url).read(), "html.parser")
    except:
      logging.error('Error reading source base from {base_url}'.format(base_url=base_url))
      raise
    source_count = 0
    for href in (a["href"] for a in soup.select("a[href]")):
        if href.endswith(
                '.jar') and 'sources.jar' in href:  # download sources jar file only
            file_name = dir_name + '/' + href
            url = base_url + '/' + href
            logging.debug('Pulling source from {url}'.format(url=url))
            pull_from_url(file_name, url, dep, incorrect_source_url)
            source_count = source_count + 1
    if source_count == 0:
      raise RuntimeError('No source found at {base_url}'.format(base_url=base_url))


@retry(reraise=True, stop=stop_after_attempt(3))
def write_to_csv(csv_list):
    csv_columns = [
        'dependency_name', 'url_to_license', 'license_type', 'source_included'
    ]
    csv_file = "{output_dir}/beam_java_dependency_list.csv".format(
        output_dir=output_dir)
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in csv_list:
                writer.writerow(data)
    except:
        traceback.print_exc()
        raise


def execute(dep):
    '''
    An example of dep.
    {
        "moduleName": "antlr:antlr",
        "moduleUrl": "http://www.antlr.org/",
        "moduleVersion": "2.7.7",
        "moduleLicense": "BSD License",
        "moduleLicenseUrl": "http://www.antlr.org/license.html"
    }
    '''

    name = dep['moduleName'].split(':')[1]
    version = dep['moduleVersion']
    name_version = name + '-' + version
    # javac is not a runtime dependency
    if name == 'javac':
      logging.debug('Skipping', name_version)
      return
    # skip self dependencies
    if dep['moduleName'].lower().startswith('beam'):
      logging.debug('Skipping', name_version)
      return
    dir_name = '{output_dir}/{name_version}.jar'.format(
        output_dir=output_dir, name_version=name_version)

    # if auto pulled, directory is existing at {output_dir}
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
        # pull license
        try:
            license_url = dep_config[name][version]['license']
        except:
            try:
                license_url = dep['moduleLicenseUrl']
            except:
                # url cannot be found, add to no_licenses and skip to pull.
                with thread_lock:
                    no_licenses.append(name_version)
                license_url = 'skip'
        pull_from_url(dir_name + '/LICENSE', license_url, name_version,
                      no_licenses)
        # pull notice
        try:
            notice_url = dep_config[name][version]['notice']
            pull_from_url(dir_name + '/NOTICE', notice_url, name_version)
        except:
            pass
    else:
        try:
            license_url = dep['moduleLicenseUrl']
        except:
            license_url = ''
        logging.debug(
            'License/notice for {name_version} were pulled automatically.'.
            format(name_version=name_version))

    # get license_type to decide if pull source code.
    try:
        license_type = dep['moduleLicense']
    except:
        try:
            license_type = dep_config[name][version]['type']
        except:
            license_type = 'no_license_type'
            with thread_lock:
                no_license_type.append(name_version)

    # pull source code if license_type is one of SOURCE_CODE_REQUIRED_LICENSES.
    if any(x in license_type.lower() for x in SOURCE_CODE_REQUIRED_LICENSES):
        try:
            base_url = dep_config[name][version]['source']
        except:
            module = dep['moduleName'].split(':')[0].replace('.', '/')
            base_url = maven_url_temp.format(module=module + '/' + name,
                                             version=version)
        pull_source_code(base_url, dir_name, name_version)
        source_included = True
    else:
        source_included = False

    csv_dict = {
        'dependency_name': name_version,
        'url_to_license': license_url,
        'license_type': license_type,
        'source_included': source_included
    }
    with thread_lock:
        csv_list.append(csv_dict)


if __name__ == "__main__":
    start = datetime.now()
    parser = argparse.ArgumentParser()
    parser.add_argument('--license_index', required=True)
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--dep_url_yaml', required=True)
    parser.add_argument('--manual_license_path', required=True)

    args = parser.parse_args()
    license_index = args.license_index
    output_dir = args.output_dir
    dep_url_yaml = args.dep_url_yaml
    manual_license_path = args.manual_license_path

    logging.getLogger().setLevel(logging.INFO)

    # index.json is generated by Gradle plugin.
    with open(license_index) as f:
        dependencies = json.load(f)

    with open(dep_url_yaml) as file:
        dep_config = yaml.full_load(file)

    maven_url_temp = 'https://repo1.maven.org/maven2/{module}/{version}'

    csv_list = []
    no_licenses = []
    no_license_type = []
    incorrect_source_url = []

    logging.info(
        'Pulling license for {num_deps} dependencies using {num_threads} threads.'
        .format(num_deps=len(dependencies['dependencies']),
                num_threads=THREADS))
    thread_lock = threading.Lock()
    pool = ThreadPool(THREADS)
    pool.map(execute, dependencies['dependencies'])

    write_to_csv(csv_list)

    error_msg = []
    run_status = 'succeed'
    if no_licenses:
        logging.error(no_licenses)
        how_to = '**************************************** ' \
                 'Licenses were not able to be pulled ' \
                 'automatically for some dependencies. Please search source ' \
                 'code of the dependencies on the internet and add "license" ' \
                 'and "notice" (if available) field to {yaml_file} for each ' \
                 'missing license. Dependency List: [{dep_list}]'.format(
            dep_list=','.join(sorted(no_licenses)), yaml_file=dep_url_yaml)
        logging.error(how_to)
        error_msg.append(how_to)
        run_status = 'failed'

    if no_license_type:
        how_to = '**************************************** ' \
                 'License type of some dependencies were not ' \
                 'identified. The license type is used to decide whether the ' \
                 'source code of the dependency should be pulled or not. ' \
                 'Please add "type" field to {yaml_file} for each dependency. ' \
                 'Dependency List: [{dep_list}]'.format(
            dep_list=','.join(sorted(no_license_type)), yaml_file=dep_url_yaml)
        error_msg.append(how_to)
        run_status = 'failed'

    if incorrect_source_url:
        how_to = '**************************************** ' \
                 'Urls to maven repo for some dependencies ' \
                 'were not able to be generated automatically. Please add ' \
                 '"source" field to {yaml_file} for each dependency. ' \
                 'Dependency List: [{dep_list}]'.format(
            dep_list=','.join(sorted(incorrect_source_url)),
            yaml_file=dep_url_yaml)
        error_msg.append(how_to)
        run_status = 'failed'

    end = datetime.now()
    logging.info(
        'pull_licenses_java.py {status}. It took {sec} seconds with {threads} threads.'
        .format(status=run_status,
                sec=(end - start).total_seconds(),
                threads=THREADS))

    if error_msg:
        raise RuntimeError('{n} error(s) occurred.'.format(n=len(error_msg)),
                           error_msg)
