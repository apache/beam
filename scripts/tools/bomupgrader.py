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
"""
This Python script is used for upgrading the GCP-BOM in BeamModulePlugin.
Specifically, it

1. preprocessing BeamModulePlugin.groovy to decide the dependencies need to sync
2. generate an empty Maven project to fetch the exact target versions to change
3. Write back to BeamModulePlugin.groovy
4. Update libraries-bom version on sdks/java/container/license_scripts/dep_urls_java.yaml

There are few reasons we need to declare the version numbers:
1. Sync the dependency that not included in GCP-BOM with those included with BOM
  For example, "com.google.cloud:google-cloud-spanner" does while "com.google.cloud:google-cloud-spanner:():test" doesn't
2. There are Beam artifacts not depending on GCP-BOM but used dependency managed
  by GCP-BOM.

Refer to https://github.com/googleapis/java-cloud-bom/tags for the dependency
versions managed by gcp-cloud-bom
"""

# To format: yapf --style sdks/python/setup.cfg --in-place scripts/tools/bomupgrader.py


class BeamModulePluginProcessor:
  # Known dependencies managed by GCP BOM and also used by Beam.
  # We only need to have one dependency for each project to figure out the target version
  KNOWN_DEPS = {
      "arrow": "org.apache.arrow:arrow-memory-core",
      "gax": "com.google.api:gax",
      "google_cloud_spanner": "com.google.cloud:google-cloud-spanner",
      "grpc":
          "io.grpc:grpc-netty",  # use "grpc-netty" to pick up proper netty version
      "netty": "io.netty:netty-transport",
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

  def write_license_script(self):
    logging.info("-----Update dep_urls_java.yaml-----")
    with open(os.path.join(self.project_root, self.LICENSE_SC_PATH),
              'r') as fin:
      lines = fin.readlines()
    with open(os.path.join(self.project_root, self.LICENSE_SC_PATH),
              'w') as fout:
      for idx, line in enumerate(lines):
        if line.strip() == 'libraries-bom:':
          lines[idx + 1] = re.sub(
              r'[\'"]\d[\.\d]+[\'"]', f"'{self.bom_version}'", lines[idx + 1])
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
    print("Usage: python scripts/tools/bomupgrader.py target_version")
    exit(1)
  processor = BeamModulePluginProcessor(sys.argv[1])
  processor.run()
