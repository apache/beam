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

import urllib.request
import re
import os

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
    match = re.search(r'google_cloud_platform_libraries_bom\s*:\s*[\"\']com\.google\.cloud:libraries-bom:([0-9.]+)[\"\']', content)
    if match:
        return match.group(1)
    raise RuntimeError("Could not find current libraries-bom in BeamModulePlugin.groovy")

def to_tuple(version_str):
    parts = []
    for part in version_str.split('.'):
        match = re.match(r'^(\d+)', part)
        parts.append(int(match.group(1)) if match else 0)
    return tuple(parts)

def main():
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

if __name__ == '__main__':
    main()
