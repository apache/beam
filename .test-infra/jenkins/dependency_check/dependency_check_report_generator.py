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

import sys
import os.path
import re
import traceback
import logging
from datetime import datetime
from bigquery_client_utils import BigQueryClientUtils


_MAX_STALE_DAYS = 360
_MAX_MINOR_VERSION_DIFF = 3
_PYPI_URL = "https://pypi.org/project/"
_MAVEN_CENTRAL_URL = "http://search.maven.org/#search|gav|1|"

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
  except Exception, e:
    raise


def extract_single_dep(dep):
  """
  Extract a single dependency check record from Java and Python reports.
  Args:
    dep: e.g "- org.assertj:assertj-core [2.5.0 -> 3.10.0]".
  Return:
    dependency name, current version, latest version.
  """
  pattern = " - ([\s\S]*)\[([\s\S]*) -> ([\s\S]*)\]"
  match = re.match(pattern, dep)
  if match is None:
    raise InvalidFormatError("Failed to extract the dependency information: {}".format(dep))
  return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()


def prioritize_dependencies(deps, sdk_type, project_id, dataset_id, table_id):
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
  high_priority_deps = []
  bigquery_client = BigQueryClientUtils(project_id, dataset_id, table_id)

  for dep in deps:
    try:
      logging.info("Start processing: %s", dep)
      dep_name, curr_ver, latest_ver = extract_single_dep(dep)
      curr_release_date, latest_release_date = query_dependency_release_dates(bigquery_client,
                                                                              dep_name,
                                                                              curr_ver,
                                                                              latest_ver)
      if sdk_type == 'Java':
        # extract the groupid and artifactid
        group_id, artifact_id = dep_name.split(":")
        dep_details_url = "{0}g:\"{1}\" AND a:\"{2}\"".format(_MAVEN_CENTRAL_URL, group_id, artifact_id)
      else:
        dep_details_url = _PYPI_URL + dep_name

      dep_info = """<tr>
        <td><a href=\'{0}\'>{1}</a></td>
        <td>{2}</td>
        <td>{3}</td>
        <td>{4}</td>
        <td>{5}</td>
        </tr>\n""".format(dep_details_url,
                          dep_name,
                          curr_ver,
                          latest_ver,
                          curr_release_date,
                          latest_release_date)
      if compare_dependency_versions(curr_ver, latest_ver):
        high_priority_deps.append(dep_info)
      elif compare_dependency_release_dates(curr_release_date, latest_release_date):
        high_priority_deps.append(dep_info)
    except:
      traceback.print_exc()
      continue

  bigquery_client.clean_stale_records_from_table()
  return high_priority_deps


def compare_dependency_versions(curr_ver, latest_ver):
  """
  Compare the current using version and the latest version.
  Return true if a major version change was found, or 3 minor versions that the current version is behind.
  Args:
    curr_ver
    latest_ver
  Return:
    boolean
  """
  if curr_ver is None or latest_ver is None:
    return True
  else:
    curr_ver_splitted = curr_ver.split('.')
    latest_ver_splitted = latest_ver.split('.')
    curr_major_ver = curr_ver_splitted[0]
    latest_major_ver = latest_ver_splitted[0]
    # compare major versions
    if curr_major_ver != latest_major_ver:
      return True
    # compare minor versions
    else:
      curr_minor_ver = curr_ver_splitted[1] if len(curr_ver_splitted) > 1 else None
      latest_minor_ver = latest_ver_splitted[1] if len(latest_ver_splitted) > 1 else None
      if curr_minor_ver is not None and latest_minor_ver is not None:
        if (not curr_minor_ver.isdigit() or not latest_minor_ver.isdigit()) and curr_minor_ver != latest_minor_ver:
          return True
        elif int(curr_minor_ver) + _MAX_MINOR_VERSION_DIFF <= int(latest_minor_ver):
          return True
     # TODO: Comparing patch versions if needed.
  return False


def query_dependency_release_dates(bigquery_client, dep_name, curr_ver_in_beam, latest_ver):
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
  if curr_release_date is None or latest_release_date is None:
    return True
  else:
    if (latest_release_date - curr_release_date).days >= _MAX_STALE_DAYS:
      return True
  return False


def generate_report(file_path, sdk_type, project_id, dataset_id, table_id):
  """
  Write SDK dependency check results into a html report.
  Args:
    file_path: the path that report will be write into.
    sdk_type: String [Java, Python, TODO: Go]
    project_id: the gcloud project ID that is used for BigQuery API requests.
    dataset_id: the BigQuery dataset ID.
    table_id: the BigQuery table ID.
  """
  report_name = 'build/dependencyUpdates/beam-dependency-check-report.html'

  if os.path.exists(report_name):
    append_write = 'a'
  else:
    append_write = 'w'

  try:
    # Extract dependency check results from build/dependencyUpdate
    report = open(report_name, append_write)
    if os.path.isfile(file_path):
      outdated_deps = extract_results(file_path)
    else:
      report.write("Did not find the raw report of dependency check: {}".format(file_path))
      report.close()
      return

    # Prioritize dependencies by comparing versions and release dates.
    high_priority_deps = prioritize_dependencies(outdated_deps, sdk_type, project_id, dataset_id, table_id)

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
  except Exception, e:
    report.write('<p> {0} </p>'.format(str(e)))

  report.close()
  logging.info("Dependency check on {0} SDK complete. The report is created.".format(sdk_type))

def main(args):
  """
  Main method.
  Args:
    args[0]: path of the raw report generated by Java/Python dependency check. Typically in build/dependencyUpdates
    args[1]: type of the check [Java, Python]
    args[2]: google cloud project id
    args[3]: BQ dataset id
    args[4]: BQ table id
  """
  generate_report(args[0], args[1], args[2], args[3], args[4])


if __name__ == '__main__':
  main(sys.argv[1:])
