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

There are few reasons we need to declare the version numbers:
1. Sync the dependency that not included in GCP-BOM with those included with BOM
  For example, "com.google.cloud:google-cloud-spanner" does while "com.google.cloud:google-cloud-spanner:():test" doesn't
2. There are Beam artifacts not depending on GCP-BOM but used dependency managed
  by GCP-BOM.
"""


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
    "com.google.cloud:google-cloud-bigquery"  # uses arrow
  ]

  # e.g. // Try to keep grpc_version consistent with gRPC version in google_cloud_platform_libraries_bom
  ANCHOR = re.compile(
      r'^\s*// Try to keep .+ consistent .+ google_cloud_platform_libraries_bom\s*$'
  )
  # e.g.  def grpc_version = "1.61.0"
  VERSION_STRING = re.compile(
      r'^\s*def (\w+)_version\s*=\s*[\'"](\S+)[\'"]')
  BOM_VERSION_STRING = re.compile(
      r'\s*google_cloud_platform_libraries_bom\s*:\s*[\'"]com\.google\.cloud:libraries-bom:([0-9\.]+)[\'"],?'
  )
  BUILD_DIR = 'build/dependencyResolver'
  GRADLE_TEMPLATE = """
plugins { id 'java' }
repositories { mavenCentral() }
dependencies {
implementation platform('com.google.cloud:libraries-bom:%s')
%s
}
configurations.implementation.canBeResolved = true
"""

  def __init__(
      self,
      bom_version,
      filepath='buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy',
      runnable=None):
    self.bom_version = bom_version
    self.filepath = filepath
    self.runnable = runnable or os.path.abspath('gradlew')
    logging.info('-----Read BeamModulePlugin-----')
    with open(filepath, 'r') as fin:
      self.original_lines = fin.readlines()
    # e.g. {"io.grpc:grpc-netty", "1.61.0"}
    self.dep_versions = {}
    self.dep_versions_current = {}

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
    assert sorted(self.KNOWN_DEPS.keys()) == sorted(found_deps.keys())
    self.dep_versions_current = {
        self.KNOWN_DEPS[k]: v for k, v in found_deps.items()
    }

  def prepare_gradle(self, bom_version):
    logging.info("-----prepare build.gradle for example project-----")
    try:
      os.makedirs(self.BUILD_DIR)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

    deps = []
    for dep in list(self.KNOWN_DEPS.values()) + self.OTHER_CONSTRANTS:
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
        *('-q dependencies --configuration implementation --console=plain'
          .split())
    ],
                          cwd=self.BUILD_DIR,
                          stdout=subprocess.PIPE)

    result = subp.stdout.decode('utf-8')
    # example line: |    +--- com.google.guava:guava:32.1.3-android -> 32.1.3-jre (*)
    logging.debug(result)
    for line in result.splitlines():
      for id in self.KNOWN_DEPS.values():
        idx = line.find(id + ':')
        if idx == -1:
          continue
        dep_and_other = line[idx + len(id) + 1:].split()
        try:
          jdx = dep_and_other.index('->')
          ver = dep_and_other[jdx + 1]
        except ValueError:
          # there might be multiple ':', e.g. come.group.id:some-package:test:1.2.3
          ver = dep_and_other[0].split(':')[-1]
        self.dep_versions[id] = ver
        break

    if len(self.dep_versions) < len(self.KNOWN_DEPS):
      logging.warning("Warning: not all dependencies are resolved: %s",
                      self.dep_versions)
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
      else:
        # replace GCP BOM version
        n = self.BOM_VERSION_STRING.match(line)
        if n:
          self.target_lines[idx] = self.original_lines[idx].replace( n.group(1), self.bom_version)
          found_bom = True

    if not found_bom:
      logging.warning('GCP_BOM version declaration not found in BeamModulePlugin')

    with open(self.filepath, 'w') as fout:
      for line in self.target_lines:
        fout.write(line)

  def run(self):
    self.check_dependencies()
    self.prepare_gradle(self.bom_version)
    self.resolve()
    self.write_back()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  if len(sys.argv) < 2:
    print("Usage: python scripts/tools/gcpbomupgrader.py target_version")
    exit(1)
  processor = BeamModulePluginProcessor(sys.argv[1])
  processor.run()
