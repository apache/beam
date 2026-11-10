<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Container VEX Automation

This directory maintains the canonical Vulnerability Exploitability eXchange (VEX) configuration and automation for all container images published by the Apache Beam project (`beam_go_sdk`, `beam_python*_sdk`, `beam_java*_sdk`, `beam_typescript_sdk`, `beam_yaml_sdk`, `beam_prism`, and `beam-jupyterlab-notebook`).

## Components

- `beam_containers_vex.yaml`: Single source-of-truth YAML mapping container families and shared OS base-layer rule sets to CISA VEX justifications (`vulnerable_code_not_in_execute_path`, `vulnerable_code_not_present`, `component_not_present`).
- `vex_manager.py`: CLI tool that:
  1. **`generate`**: Produces **OpenVEX v0.2.0** (`openvex/*.openvex.json`), **CSAF 2.0 VEX** (`csaf/*.vex.json`, with multi-registry `product_tree.branches` and `notes[].title` entries compatible with `gcloud artifacts vulnerabilities load-vex`), and AutoVM/Lichene triage rule snippets (`lichene/beam_vex_rules.pi`).
  2. **`scan`**: Scans published container images across **Google Artifact Registry** (`gcloud artifacts vulnerabilities list`), **Trivy** (`trivy image --format json`), and **`govulncheck`** (`govulncheck -mode binary`), diffs discovered CVEs against `beam_containers_vex.yaml`, performs automated + optional LLM reachability triage, and updates `beam_containers_vex.yaml` when new non-exploitable CVEs are verified.
  3. **`attach`**: Attaches OpenVEX OCI attestations via `cosign attest --yes --type https://openvex.dev/ns` and loads CSAF 2.0 VEX notes into Artifact Registry via `gcloud artifacts vulnerabilities load-vex`.

## Usage

```bash
# 1. Generate OpenVEX, CSAF 2.0, and Lichene rules from YAML
python3 sdks/containers/vex/vex_manager.py generate

# 2. Scan an image in Artifact Registry + Trivy + govulncheck and update config
python3 sdks/containers/vex/vex_manager.py scan \
  --family=beam_go_sdk \
  --ar-image=gcr.io/apache-beam-testing/updated_released_container_images/beam_go_sdk:latest \
  --trivy-image=apache/beam_go_sdk:latest \
  --agentic-triage \
  --update-config

# 3. Attach OpenVEX attestation and Artifact Registry CSAF VEX
python3 sdks/containers/vex/vex_manager.py attach \
  --image-uri=gcr.io/apache-beam-testing/updated_released_container_images/beam_go_sdk@sha256:<DIGEST> \
  --openvex=sdks/containers/vex/openvex/beam_go_sdk.openvex.json \
  --csaf=sdks/containers/vex/csaf/beam_go_sdk.vex.json
```
