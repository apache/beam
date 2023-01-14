#!/usr/bin/env python
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

import dependency_check.version_comparer as version_comparer
import logging
import os.path
import re
import requests
import sys
import traceback

from datetime import datetime
from dependency_check.bigquery_client_utils import BigQueryClientUtils
from dependency_check.report_generator_config import ReportGeneratorConfig
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


logging.getLogger().setLevel(logging.INFO)

class InvalidFormatError(Exception):
  def __init__(self, message):
    super(InvalidFormatError, self).__init__(message)


def extract_results(file_path):
  """
  Extract the Java/Python dependency reports and return a collection of out-of-date dependencies.
  Args:
    file_path: the path of the raw reports
  Return:
    outdated_deps: a collection of dependencies that has updates
  """
  outdated_deps = []
  try:
    with open(file_path) as raw_report:
      see_oudated_deps = False
      for line in raw_report:
        if see_oudated_deps:
          outdated_deps.append(line)
        if line.startswith('The following dependencies have later '):
          see_oudated_deps = True
    raw_report.close()
    return outdated_deps
  except:
    raise


def extract_single_dep(dep):
  """
  Extract a single dependency check record from Java and Python reports.
  Args:
    dep: e.g " - org.assertj:assertj-core [2.5.0 -> 3.10.0]".
  Return:
    dependency name, current version, latest version.
  """
  pattern = " - ([\s\S]*)\[([\s\S]*) -> ([\s\S]*)\]"
  match = re.match(pattern, dep)
  if match is None:
    raise InvalidFormatError("Failed to extract the dependency information: {}".format(dep))
  return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()


def prioritize_dependencies(deps, sdk_type):
  """
  Extracts and analyze dependency versions and release dates.
  Returns a collection of dependencies which is "high priority" in html format:
    1. dependency has major release. e.g org.assertj:assertj-core [2.5.0 -> 3.10.0]
    2. dependency is 3 sub-versions behind the newest one. e.g org.tukaani:xz [1.5 -> 1.8]
    3. dependency has not been updated for more than 6 months.

  Args:
    deps: A collection of outdated dependencies.
  Return:
    high_priority_deps: A collection of dependencies which need to be taken care of before next release.
  """

  project_id = ReportGeneratorConfig.GCLOUD_PROJECT_ID
  dataset_id = ReportGeneratorConfig.DATASET_ID
  table_id = ReportGeneratorConfig.get_bigquery_table_id(sdk_type)
  high_priority_deps = []
  bigquery_client = BigQueryClientUtils(project_id, dataset_id, table_id)

  for dep in deps:
    try:
      if re.match(r'https?://', dep.lstrip()):
        # Gradle-version-plugin's output contains URLs of the libraries
        continue
      logging.info("\n\nStart processing: " + dep)
      dep_name, curr_ver, latest_ver = extract_single_dep(dep)
      curr_release_date = None
      latest_release_date = None
      group_id = None

      if sdk_type == 'Java':
        # extract the groupid and artifactid
        group_id, artifact_id = dep_name.split(":")
        dep_details_url = "{0}/{1}/{2}".format(ReportGeneratorConfig.MAVEN_CENTRAL_URL, group_id, artifact_id)
        curr_release_date = find_release_time_from_maven_central(group_id, artifact_id, curr_ver)
        latest_release_date = find_release_time_from_maven_central(group_id, artifact_id, latest_ver)
      else:
        dep_details_url = ReportGeneratorConfig.PYPI_URL + dep_name
        curr_release_date = find_release_time_from_python_compatibility_checking_service(dep_name, curr_ver)
        latest_release_date = find_release_time_from_python_compatibility_checking_service(dep_name, latest_ver)

      if not curr_release_date or not latest_release_date:
        curr_release_date, latest_release_date = query_dependency_release_dates_from_bigquery(bigquery_client,
                                                                                dep_name,
                                                                                curr_ver,
                                                                                latest_ver)
      dep_info = """<tr>
        <td><a href=\'{0}\'>{1}</a></td>
        <td>{2}</td>
        <td>{3}</td>
        <td>{4}</td>
        <td>{5}</td>""".format(dep_details_url,
                          dep_name,
                          curr_ver,
                          latest_ver,
                          curr_release_date,
                          latest_release_date)
      if (version_comparer.compare_dependency_versions(curr_ver, latest_ver) or
          compare_dependency_release_dates(curr_release_date, latest_release_date)):
        high_priority_deps.append(dep_info)

    except:
      traceback.print_exc()
      continue

  bigquery_client.clean_stale_records_from_table()
  return high_priority_deps


def find_release_time_from_maven_central(group_id, artifact_id, version):
  """
  Find release dates from Maven Central REST API.
  Args:
    group_id:
    artifact_id:
    version:
  Return:
    release date
  """
  url = "http://search.maven.org/solrsearch/select?q=g:{0}+AND+a:{1}+AND+v:{2}".format(
      group_id,
      artifact_id,
      version
  )
  logging.info('Finding release date of {0}:{1} {2} from the Maven Central'.format(
      group_id,
      artifact_id,
      version
  ))
  try:
    response = request_session_with_retries().get(url)
    if not response.ok:
      logging.error("""Failed finding the release date of {0}:{1} {2}.
        The response status code is not ok: {3}""".format(group_id,
                                                          artifact_id,
                                                          version,
                                                          str(response.status_code)))
      return None
    response_data = response.json()
    release_timestamp_mills = response_data['response']['docs'][0]['timestamp']
    release_date = datetime.fromtimestamp(release_timestamp_mills/1000).date()
    return release_date
  except Exception as e:
    logging.error("Errors while extracting the release date: " + str(e))
    return None


def find_release_time_from_python_compatibility_checking_service(dep_name, version):
  """
  Query release dates by using Python compatibility checking service.
  Args:
    dep_name:
    version:
  Return:
    release date
  """
  url = 'http://104.197.8.72/?package={0}=={1}&python-version=2'.format(
      dep_name,
      version
  )
  logging.info('Finding release time of {0} {1} from the python compatibility checking service.'.format(
      dep_name,
      version
  ))
  try:
    response = request_session_with_retries().get(url)
    if not response.ok:
      logging.error("""Failed finding the release date of {0} {1}. 
        The response status code is not ok: {2}""".format(dep_name,
                                                          version,
                                                          str(response.status_code)))
      return None
    response_data = response.json()
    release_datetime = response_data['dependency_info'][dep_name]['installed_version_time']
    release_date = datetime.strptime(release_datetime, '%Y-%m-%dT%H:%M:%S').date()
    return release_date
  except Exception as e:
    logging.error("Errors while extracting the release date: " + str(e))
    return None


def request_session_with_retries():
  """
  Create an http session with retries
  """
  session = requests.Session()
  retries = Retry(total=3)
  session.mount('http://', HTTPAdapter(max_retries=retries))
  session.mount('https://', HTTPAdapter(max_retries=retries))
  return session


def query_dependency_release_dates_from_bigquery(bigquery_client, dep_name, curr_ver_in_beam, latest_ver):
  """
  Query release dates of current version and the latest version from BQ tables.
  Args:
    bigquery_client: a bq client object that bundle configurations for API requests
    dep_name: dependency name
    curr_ver_in_beam: the current version used in beam
    latest_ver: the later version
  Return:
    A tuple that contains `curr_release_date` and `latest_release_date`.
  """
  try:
    curr_release_date, is_currently_used_bool = bigquery_client.query_dep_info_by_version(dep_name, curr_ver_in_beam)
    latest_release_date, _ = bigquery_client.query_dep_info_by_version(dep_name, latest_ver)
    date_today = datetime.today().date()

    # sync to the bigquery table on the dependency status of the currently used version.
    if not is_currently_used_bool:
      currently_used_version_in_db, currently_used_release_date_in_db = bigquery_client.query_currently_used_dep_info_in_db(dep_name)
      if currently_used_version_in_db is not None:
        bigquery_client.delete_dep_from_table(dep_name, currently_used_version_in_db)
        bigquery_client.insert_dep_to_table(dep_name, currently_used_version_in_db, currently_used_release_date_in_db, is_currently_used=False)
      if curr_release_date is None:
        bigquery_client.insert_dep_to_table(dep_name, curr_ver_in_beam, date_today, is_currently_used=True)
      else:
        bigquery_client.delete_dep_from_table(dep_name, curr_ver_in_beam)
        bigquery_client.insert_dep_to_table(dep_name, curr_ver_in_beam, curr_release_date, is_currently_used=True)
    # sync to the bigquery table on the dependency status of the latest version.
    if latest_release_date is None:
      bigquery_client.insert_dep_to_table(dep_name, latest_ver, date_today, is_currently_used=False)
      latest_release_date = date_today
  except Exception:
    raise
  return curr_release_date, latest_release_date


def compare_dependency_release_dates(curr_release_date, latest_release_date):
  """
  Compare release dates of current using version and the latest version.
  Return true if the current version is behind over 60 days.
  Args:
    curr_release_date
    latest_release_date
  Return:
    boolean
  """
  if not curr_release_date or not latest_release_date:
    return False
  else:
    if (latest_release_date - curr_release_date).days >= ReportGeneratorConfig.MAX_STALE_DAYS:
      return True
  return False


def generate_report(sdk_type):
  """
  Write SDK dependency check results into a html report.
  Args:
    sdk_type: String [Java, Python, TODO: Go]
  """
  report_name = ReportGeneratorConfig.FINAL_REPORT
  raw_report = ReportGeneratorConfig.get_raw_report(sdk_type)

  if os.path.exists(report_name):
    append_write = 'a'
  else:
    append_write = 'w'

  try:
    # Extract dependency check results from build/dependencyUpdate
    report = open(report_name, append_write)
    if os.path.isfile(raw_report):
      outdated_deps = extract_results(raw_report)
    else:
      report.write("Did not find the raw report of dependency check: {}".format(raw_report))
      report.close()
      return

    # Prioritize dependencies by comparing versions and release dates.
    high_priority_deps = prioritize_dependencies(outdated_deps, sdk_type)

    # Write results to a report
    subtitle = "<h2>High Priority Dependency Updates Of Beam {} SDK:</h2>\n".format(sdk_type)
    table_fields = """<tr>
      <td><b>{0}</b></td>
      <td><b>{1}</b></td>
      <td><b>{2}</b></td>
      <td><b>{3}</b></td>
      <td><b>{4}</b></td>
      </tr>""".format("Dependency Name",
                      "Current Version",
                      "Latest Version",
                      "Release Date Of the Current Used Version",
                      "Release Date Of The Latest Release")
    report.write(subtitle)
    report.write("<table>\n")
    report.write(table_fields)
    for dep in high_priority_deps:
      report.write("%s" % dep)
    report.write("</table>\n")
  except Exception as e:
    traceback.print_exc()
    logging.error("Failed generate the dependency report. " + str(e))
    report.write('<p> {0} </p>'.format(str(e)))

  report.close()
  logging.info("Dependency check on {0} SDK complete. The report is created.".format(sdk_type))

def main(args):
  """
  Main method.
  Args:
    args[0]: type of the check [Java, Python]
  """
  generate_report(args[0])


if __name__ == '__main__':
  main(sys.argv[1:])
