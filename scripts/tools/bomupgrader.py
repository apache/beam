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

import errno
import logging
import os
import re
import subprocess
import sys
import urllib.request
"""
This Python script is used for upgrading the GCP-BOM in BeamModulePlugin.
Specifically, it

1. preprocessing BeamModulePlugin.groovy to decide the dependencies need to sync
2. generate an empty Maven project to fetch the exact target versions to change
3. Write back to BeamModulePlugin.groovy
4. Update libraries-bom and opentelemetry-bom entries on
   sdks/java/container/license_scripts/dep_urls_java.yaml

There are few reasons we need to declare the version numbers:
1. Sync the dependency that not included in GCP-BOM with those included with BOM
  For example, "com.google.cloud:google-cloud-spanner" does while "com.google.cloud:google-cloud-spanner:():test" doesn't
2. There are Beam artifacts not depending on GCP-BOM but used dependency managed
  by GCP-BOM.

Refer to https://github.com/googleapis/java-cloud-bom/tags for the dependency
versions managed by gcp-cloud-bom
"""

# To format: yapf --style sdks/python/setup.cfg --in-place scripts/tools/bomupgrader.py


def get_latest_bom():
  url = "https://repo1.maven.org/maven2/com/google/cloud/libraries-bom/maven-metadata.xml"
  with urllib.request.urlopen(url, timeout=15) as response:
    xml = response.read().decode('utf-8')
  match = re.search(r'<release>([^<]+)</release>', xml)
  if match:
    return match.group(1)
  raise RuntimeError("Could not find latest release in Maven metadata")


def get_current_bom():
  path = "buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy"
  with open(path) as f:
    content = f.read()
  match = re.search(
      r'google_cloud_platform_libraries_bom\s*:\s*[\"\']com\.google\.cloud:libraries-bom:([0-9.]+)[\"\']',
      content)
  if match:
    return match.group(1)
  raise RuntimeError(
      "Could not find current libraries-bom in BeamModulePlugin.groovy")


def to_tuple(version_str):
  parts = []
  for part in version_str.split('.'):
    match = re.match(r'^(\d+)', part)
    parts.append(int(match.group(1)) if match else 0)
  return tuple(parts)


def check_bom():
  latest = get_latest_bom()
  current = get_current_bom()
  print(f"Latest libraries-bom version: {latest}")
  print(f"Current libraries-bom version: {current}")

  should_upgrade = to_tuple(latest) > to_tuple(current)

  github_output = os.getenv('GITHUB_OUTPUT')
  if github_output:
    with open(github_output, 'a') as f:
      f.write(f"should_upgrade={str(should_upgrade).lower()}\n")
      f.write(f"latest_version={latest}\n")
      f.write(f"current_version={current}\n")

  if should_upgrade:
    print("A newer version of libraries-bom is available. Upgrade needed.")
  else:
    print("libraries-bom is up-to-date.")
  return should_upgrade, latest


class BeamModulePluginProcessor:
  # Known dependencies managed by GCP BOM and also used by Beam.
  # We only need to have one dependency for each project to figure out the target version
  KNOWN_DEPS = {
      "arrow": "org.apache.arrow:arrow-memory-core",
      "gax": "com.google.api:gax",
      "grpc":
          "io.grpc:grpc-netty",  # use "grpc-netty" to pick up proper netty version
      "netty": "io.netty:netty-transport",
      "opentelemetry": "io.opentelemetry:opentelemetry-sdk",
      "protobuf": "com.google.protobuf:protobuf-java"
  }
  # dependencies managed by GCP-BOM that used the dependencies in KNOWN_DEPS
  # So we need to add it to the example project to get the version to sync
  OTHER_CONSTRANTS = [
      "com.google.cloud:google-cloud-bigquery",  # for arrow
      'com.google.cloud:google-cloud-storage',  # for google-api-services-storage
      'com.google.cloud:google-cloud-resourcemanager',  # for google-api-services-cloudresourcemanager
      'com.google.cloud:google-cloud-datastore',  # for google-cloud-dataflow-java-proto-library-all
  ]

  # TODO: the logic can be generalized to support multiple BOM
  ANCHOR = re.compile(
      r'\s*//\s*\[bomupgrader\] determined by: (\S+), consistent with: google_cloud_platform_libraries_bom'
  )
  # e.g.  def grpc_version = "1.61.0"
  VERSION_STRING = re.compile(r'^\s*def (\w+)_version\s*=\s*[\'"](\S+)[\'"]')
  SINGLE_VERSION_STRING = re.compile(
      r'.+:\s*[\'"]([\w\-.]+:[\w\-.]+):(.+)[\'"],\s*//\s*\[bomupgrader\] sets version'
  )
  BOM_VERSION_STRING = re.compile(
      r'\s*google_cloud_platform_libraries_bom\s*:\s*[\'"]com\.google\.cloud:libraries-bom:([0-9\.]+)[\'"],?'
  )
  BUILD_DIR = 'build/dependencyResolver'
  BEAMMPLG_PATH = 'buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy'
  LICENSE_SC_PATH = 'sdks/java/container/license_scripts/dep_urls_java.yaml'
  GRADLE_TEMPLATE = """
plugins { id 'java' }
repositories { mavenCentral() }
dependencies {
implementation platform('com.google.cloud:libraries-bom:%s')
%s
}
configurations.implementation.canBeResolved = true
"""

  def __init__(self, bom_version, project_root='.', runnable=None):
    self.bom_version = bom_version
    self.project_root = project_root
    self.runnable = runnable or os.path.abspath('gradlew')
    logging.info('-----Read BeamModulePlugin-----')
    with open(os.path.join(project_root, self.BEAMMPLG_PATH), 'r') as fin:
      self.original_lines = fin.readlines()
    # e.g. {"io.grpc:grpc-netty", "1.61.0"}
    self.dep_versions = {}
    self.dep_versions_current = {}
    # dependencies managed by bomupgrader. They are declared inline in BeamModulePlugin,
    # different from KNOWN_DEPS which first define a version
    self.set_deps = {}
    self.set_deps_current = {}

  @staticmethod
  def resolve_actual_dep(line, id):
    """Resolve actual dependency from dependencyTree line"""
    idx = line.find(id + ':')
    if idx == -1: return ""  # not found
    dep_and_other = line[idx + len(id) + 1:].split()
    try:
      jdx = dep_and_other.index('->')
      ver = dep_and_other[jdx + 1]
    except ValueError:
      # there might be multiple ':', e.g. come.group.id:some-package:test:1.2.3
      ver = dep_and_other[0].split(':')[-1]
    return ver

  def check_dependencies(self):
    """Check dependencies in KNOWN_DEPS are found in BeamModulePlugin, and vice versa."""
    logging.info("-----check dependency defs in BeamModulePlugin-----")
    found_deps = {}
    for idx, line in enumerate(self.original_lines):
      m = self.ANCHOR.match(line)
      if m:
        n = self.VERSION_STRING.search(self.original_lines[idx + 1])
        if not n:
          raise RuntimeError(
              "Version definition not found after anchor comment. Try standardize it."
          )
        found_deps[n.group(1)] = n.group(2)
        continue
      m = self.SINGLE_VERSION_STRING.match(line)
      if m:
        self.set_deps_current[m.group(1)] = m.group(2)

    assert sorted(self.KNOWN_DEPS.keys()) == sorted(found_deps.keys()), f"expect {self.KNOWN_DEPS.keys()} == {found_deps.keys()}"
    self.dep_versions_current = {
        self.KNOWN_DEPS[k]: v
        for k, v in found_deps.items()
    }

  def prepare_gradle(self, bom_version):
    logging.info("-----prepare build.gradle for example project-----")
    try:
      os.makedirs(self.BUILD_DIR)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

    deps = []
    for dep in list(self.KNOWN_DEPS.values()) + self.OTHER_CONSTRANTS + list(
        self.set_deps_current.keys()):
      deps.append(f"implementation '{dep}'")
    gradle_file = self.GRADLE_TEMPLATE % (bom_version, "\n".join(deps))
    with open(os.path.join(self.BUILD_DIR, 'build.gradle'), 'w') as fout:
      fout.write(gradle_file)
    # we need a settings.gradle
    with open(os.path.join(self.BUILD_DIR, 'settings.gradle'), 'w') as fout:
      fout.write('\n')

  def resolve(self):
    logging.info("-----resolve dependency-----")
    subp = subprocess.run([
        self.runnable,
        *(
            '-q dependencies --configuration implementation --console=plain'.
            split())
    ],
                          cwd=self.BUILD_DIR,
                          stdout=subprocess.PIPE)

    result = subp.stdout.decode('utf-8')
    # example line: |    +--- com.google.guava:guava:32.1.3-android -> 32.1.3-jre (*)
    logging.debug(result)
    get_dep_line = re.compile('\s+([\w\-.]+:[\w\-.]+):(.+)')

    for line in result.splitlines():
      # search self.set_deps version
      m = get_dep_line.search(line)
      if m and m.group(1) in self.set_deps_current:
        ver = self.resolve_actual_dep(line, m.group(1))
        self.set_deps[m.group(1)] = ver
        continue

      # search KNOWN_DEPS version
      for id in self.KNOWN_DEPS.values():
        if id in self.dep_versions: continue
        ver = self.resolve_actual_dep(line, id)
        if ver:
          self.dep_versions[id] = ver
          break

    if len(self.dep_versions) < len(self.KNOWN_DEPS):
      logging.warning(
          "Warning: not all dependencies are resolved: %s", self.dep_versions)
      logging.info(result)

    if len(self.set_deps) < len(self.set_deps_current):
      logging.warning(
          "Warning: not all dependencies are resolved: %s", self.set_deps)
      logging.info(result)

  def write_back(self):
    logging.info("-----Update BeamModulePlugin-----")
    # make a shallow copy
    self.target_lines = list(self.original_lines)
    found_bom = False

    for idx, line in enumerate(self.original_lines):
      m = self.ANCHOR.match(line)
      if m:
        n = self.VERSION_STRING.search(self.original_lines[idx + 1])
        if not n:
          raise RuntimeError(
              "Version definition not found after anchor comment. Try standardize it."
          )
        id = self.KNOWN_DEPS[n.group(1)]
        new_v = self.dep_versions[id]
        old_v = self.dep_versions_current[id]
        if new_v != old_v:
          self.target_lines[idx + 1] = self.original_lines[idx + 1].replace(
              old_v, new_v)
          logging.info('Changed %s: %s -> %s', id, old_v, new_v)
        else:
          logging.info('Unchanged: %s:%s', id, new_v)
        continue

      # single_ver replace
      m = self.SINGLE_VERSION_STRING.match(line)
      if m:
        id = m.group(1)
        old_v = m.group(2)
        new_v = self.set_deps[id]
        if new_v != old_v:
          self.target_lines[idx] = self.original_lines[idx].replace(
              old_v, new_v)
          logging.info('Changed %s: %s -> %s', id, old_v, new_v)
        else:
          logging.info('Unchanged: %s:%s', id, new_v)
      # replace GCP BOM version
      n = self.BOM_VERSION_STRING.match(line)
      if n:
        self.target_lines[idx] = self.original_lines[idx].replace(
            n.group(1), self.bom_version)
        found_bom = True

    if not found_bom:
      logging.warning(
          'GCP_BOM version declaration not found in BeamModulePlugin')

    with open(os.path.join(self.project_root, self.BEAMMPLG_PATH), 'w') as fout:
      for line in self.target_lines:
        fout.write(line)

  def _get_opentelemetry_version(self):
    for line in self.target_lines:
      match = self.VERSION_STRING.match(line)
      if match and match.group(1) == 'opentelemetry':
        return match.group(2)
    logging.warning('opentelemetry_version not found in BeamModulePlugin')
    return None

  def write_license_script(self):
    logging.info("-----Update dep_urls_java.yaml-----")
    license_path = os.path.join(self.project_root, self.LICENSE_SC_PATH)
    with open(license_path, 'r') as fin:
      lines = fin.readlines()

    otel_version = self._get_opentelemetry_version()
    otel_license_url = (
        'https://raw.githubusercontent.com/open-telemetry/'
        'opentelemetry-java/v{}/LICENSE'.format(otel_version)
        if otel_version else None)
    otel_license_line = '    license: "{}"\n'.format(otel_license_url)

    for idx, line in enumerate(lines):
      stripped = line.strip()
      if stripped == 'libraries-bom:':
        lines[idx + 1] = re.sub(
            r'[\'"]\d[\d\.]+[\'"]', "'{}'".format(self.bom_version),
            lines[idx + 1])
        continue
      if otel_version and stripped == 'opentelemetry-bom:':
        lines[idx + 1] = re.sub(
            r"'[\d\.]+'", "'{}'".format(otel_version), lines[idx + 1])
        lines[idx + 2] = otel_license_line
        continue
      if otel_version and stripped == 'opentelemetry-bom-alpha:':
        lines[idx + 1] = re.sub(
            r"'[\d\.]+-alpha'", "'{}-alpha'".format(otel_version),
            lines[idx + 1])
        lines[idx + 2] = otel_license_line

    if otel_version:
      logging.info(
          'Updated opentelemetry-bom license entries to %s', otel_version)

    with open(license_path, 'w') as fout:
      for line in lines:
        fout.write(line)

  def run(self):
    self.check_dependencies()
    self.prepare_gradle(self.bom_version)
    self.resolve()
    self.write_back()
    self.write_license_script()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  if len(sys.argv) < 2:
    print("Usage: python scripts/tools/bomupgrader.py [--check | latest | target_version]")
    exit(1)

  arg = sys.argv[1]
  if arg in ['--check', '--check-only']:
    check_bom()
  else:
    if arg in ['latest', '--latest']:
      _, target_ver = check_bom()
    else:
      target_ver = arg
    processor = BeamModulePluginProcessor(target_ver)
    processor.run()
