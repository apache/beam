# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Unit tests for sdks/containers/vex/vex_manager.py."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import vex_manager


class VexManagerTest(unittest.TestCase):

  def test_expand_uri_variants_for_gcr_and_ar(self) -> None:
    variants = vex_manager.expand_uri_variants(
        "gcr.io/cloud-dataflow/v1beta3/beam_go_sdk",
        extra_digests=["sha256:0957e5bf"],
    )
    self.assertIn("gcr.io/cloud-dataflow/v1beta3/beam_go_sdk", variants)
    self.assertIn("us.gcr.io/cloud-dataflow/v1beta3/beam_go_sdk", variants)
    self.assertIn(
        "us-docker.pkg.dev/cloud-dataflow/gcr.io/v1beta3/beam_go_sdk",
        variants,
    )
    self.assertIn(
        "us-docker.pkg.dev/cloud-dataflow/us.gcr.io/v1beta3/beam_go_sdk",
        variants,
    )
    self.assertIn(
        "us-docker.pkg.dev/cloud-dataflow/gcr.io/v1beta3/beam_go_sdk@sha256:0957e5bf",
        variants,
    )
    port_variants = vex_manager.expand_uri_variants(
        "localhost:5000/project/image:latest"
    )
    self.assertIn("localhost:5000/project/image", port_variants)
    self.assertIn("localhost:5000/project/image:latest", port_variants)

  def test_generate_openvex_csaf_and_lichene(self) -> None:
    config_path = Path(__file__).with_name("beam_containers_vex.yaml")
    with tempfile.TemporaryDirectory() as tmpdir:
      rc = vex_manager.main([
          "generate",
          f"--config={config_path}",
          f"--output-dir={tmpdir}",
          "--timestamp=2026-11-10T00:00:00Z",
      ])
      self.assertEqual(rc, 0)

      openvex_file = Path(tmpdir) / "openvex" / "beam_go_sdk.openvex.json"
      csaf_file = Path(tmpdir) / "csaf" / "beam_go_sdk.vex.json"
      lichene_file = Path(tmpdir) / "lichene" / "beam_vex_rules.pi"

      self.assertTrue(openvex_file.exists())
      self.assertTrue(csaf_file.exists())
      self.assertTrue(lichene_file.exists())

      openvex_doc = json.loads(openvex_file.read_text(encoding="utf-8"))
      self.assertEqual(openvex_doc["@context"], "https://openvex.dev/ns/v0.2.0")
      cves = {
          s["vulnerability"]["name"] for s in openvex_doc["statements"]
      }
      self.assertIn("CVE-2019-9192", cves)
      self.assertIn("CVE-2026-95619", cves)

      csaf_doc = json.loads(csaf_file.read_text(encoding="utf-8"))
      self.assertEqual(csaf_doc["document"]["csaf_version"], "2.0")
      for vuln in csaf_doc["vulnerabilities"]:
        for note in vuln["notes"]:
          self.assertIn("title", note)
          self.assertTrue(note["title"])

      lichene_text = lichene_file.read_text(encoding="utf-8")
      self.assertIn("containers.triage_vulns(", lichene_text)
      self.assertIn("CVE-2019-9192", lichene_text)

  def test_parse_artifact_registry_and_trivy_and_classify(self) -> None:
    ar_sample = json.dumps({
        "Occurrences": [
            {
                "occurrence": {
                    "vulnerability": {
                        "shortDescription": "CVE-2026-99999",
                        "effectiveSeverity": "LOW",
                        "packageIssue": {
                            "affectedPackage": "gcc-12",
                            "packageType": "OS",
                            "effectiveSeverity": "LOW",
                            "fixAvailable": False,
                        },
                    }
                }
            }
        ]
    })
    ar_findings = vex_manager.parse_artifact_registry_vulns(ar_sample)
    self.assertEqual(len(ar_findings), 1)
    verdict = vex_manager.classify_finding_reachability(ar_findings[0])
    self.assertEqual(verdict["status"], "not_affected")
    self.assertEqual(
        verdict["justification"], "vulnerable_code_not_in_execute_path"
    )

    trivy_sample = json.dumps({
        "Results": [
            {
                "Class": "lang-pkgs",
                "Type": "gobinary",
                "Vulnerabilities": [
                    {
                        "VulnerabilityID": "CVE-2026-88888",
                        "PkgName": "golang.org/x/net",
                        "Severity": "MEDIUM",
                        "FixedVersion": "0.40.0",
                    }
                ],
            },
            {
                "Class": "lang-pkgs",
                "Type": "pip",
                "Vulnerabilities": [
                    {
                        "VulnerabilityID": "CVE-2026-77777",
                        "PkgName": "urllib3",
                        "Severity": "HIGH",
                        "FixedVersion": "2.2.2",
                    }
                ],
            },
        ]
    })
    trivy_findings = vex_manager.parse_trivy_vulns(trivy_sample)
    self.assertEqual(len(trivy_findings), 2)
    go_verdict = vex_manager.classify_finding_reachability(
        trivy_findings[0], reachable_go_cves=set()
    )
    self.assertEqual(go_verdict["status"], "not_affected")
    py_verdict = vex_manager.classify_finding_reachability(
        trivy_findings[1], reachable_go_cves=set()
    )
    self.assertEqual(py_verdict["status"], "under_investigation")


if __name__ == "__main__":
  unittest.main()
