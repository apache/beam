#!/usr/bin/env python3
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

"""Generates, scans/triages, and attaches VEX statements for Beam containers.

Supported capabilities:
  1. `generate`: Produces OpenVEX v0.2.0 JSON, CSAF 2.0 VEX JSON (compatible
     with `gcloud artifacts vulnerabilities load-vex`), and AutoVM / Lichene
     triage rule snippets from `beam_containers_vex.yaml`.
  2. `scan`: Queries vulnerabilities from Google Artifact Registry
     (`gcloud artifacts vulnerabilities list`), Trivy (`trivy image`), and
     `govulncheck`, filters out CVEs already covered by VEX, performs automated
     and optional LLM-assisted reachability triage, and optionally appends
     newly verified `not_affected` entries to the VEX config.
  3. `attach`: Attaches OpenVEX attestations to container digests via
     `cosign attest --type https://openvex.dev/ns` and loads CSAF 2.0 VEX
     assessments into Artifact Registry via
     `gcloud artifacts vulnerabilities load-vex`.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request
from typing import Any
import yaml

CSAF_JUSTIFICATION_MAP = {
    "component_not_present": "component_not_present",
    "vulnerable_code_not_present": "vulnerable_code_not_present",
    "vulnerable_code_not_in_execute_path": "vulnerable_code_not_in_execute_path",
    "vulnerable_code_cannot_be_controlled_by_adversary": (
        "vulnerable_code_cannot_be_controlled_by_adversary"
    ),
    "inline_mitigations_already_exist": "inline_mitigations_already_exist",
}

LICHENE_STATUS_MAP = {
    "component_not_present": "COMPONENT_NOT_PRESENT",
    "vulnerable_code_not_present": "CODE_NOT_PRESENT",
    "vulnerable_code_not_in_execute_path": "CODE_UNREACHABLE",
    "vulnerable_code_cannot_be_controlled_by_adversary": "CODE_NOT_CONTROLLABLE",
    "inline_mitigations_already_exist": "MITIGATED",
}

BUILD_ONLY_OS_PACKAGES = {
    "binutils": (
        "GNU binutils CLI utilities (objdump, readelf, nm, ld, strip) and "
        "libbfd are build-time tools inherited from the base OS image and are "
        "never invoked on untrusted ELF inputs at runtime."
    ),
    "elfutils": (
        "elfutils DWARF/ELF inspection utilities are build/debug tools not "
        "executed at runtime by the container entrypoint."
    ),
    "gcc-12": (
        "gcc-12 compiler binaries and C++ demangler CLI utilities are "
        "build-time tools not invoked at runtime."
    ),
    "linux-libc-dev": (
        "linux-libc-dev contains C header files for compiling userspace "
        "programs and ships no runtime executable code or shared libraries."
    ),
    "m4": (
        "GNU m4 is a build-time macro processor pulled in by autotools and is "
        "never invoked at runtime."
    ),
    "toolchain": (
        "Go cmd/go build-time toolchain vulnerability reported from embedded "
        "Go binary build metadata; the runtime container image does not ship "
        "or execute cmd/go."
    ),
    "bluez": (
        "Bluetooth protocol stack libraries present in the upstream Debian "
        "base image are unused by container worker processes."
    ),
    "cairo": (
        "2D desktop graphics library present in the upstream Debian base "
        "image is not loaded by headless worker processes."
    ),
    "gdk-pixbuf": (
        "Desktop GUI pixel-buffer library in the upstream Debian base image "
        "is unused by headless worker processes."
    ),
    "imagemagick": (
        "ImageMagick CLI/library in the upstream Debian base image is not "
        "invoked by the container entrypoint."
    ),
    "libheif": (
        "HEIF image decoder library in the upstream Debian base image is not "
        "loaded by headless worker processes."
    ),
    "libwmf": (
        "Windows Metafile library in the upstream Debian base image is not "
        "loaded by headless worker processes."
    ),
    "openexr": (
        "OpenEXR HDR image library in the upstream Debian base image is not "
        "loaded by headless worker processes."
    ),
    "openjpeg2": (
        "JPEG 2000 image library in the upstream Debian base image is not "
        "loaded by headless worker processes."
    ),
}

GCR_HOST_RE = re.compile(
    r"^(?P<host>(?:(?P<prefix>us|eu|asia)\.)?gcr\.io)/"
    r"(?P<project>(?:[^/@:.]+\.[^/@:]+/[^/@:]+|[^/@:]+))/(?P<image>.+)$"
)
AR_REDIRECTED_GCR_RE = re.compile(
    r"^(?P<region>us|europe|asia)-docker\.pkg\.dev/"
    r"(?P<project>(?:[^/@:.]+\.[^/@:]+/[^/@:]+|[^/@:]+))/"
    r"(?P<repo>(?:(?:us|eu|asia)\.)?gcr\.io)/(?P<image>.+)$"
)


def expand_uri_variants(uri: str, extra_digests: list[str] | None = None) -> list[str]:
  """Expands a container repository URI into all GCR / AR forms needed by gcloud load-vex."""
  cleaned = re.sub(r"^https?://", "", uri.strip())
  base_uri = cleaned.split("@")[0].split(":")[0]
  explicit_suffix = ""
  if "@" in cleaned:
    explicit_suffix = "@" + cleaned.split("@", 1)[1]
  elif ":" in cleaned.split("/")[-1]:
    explicit_suffix = ":" + cleaned.split("/")[-1].split(":", 1)[1]

  base_variants: list[str] = [base_uri]

  m_gcr = GCR_HOST_RE.match(base_uri)
  if m_gcr:
    host = m_gcr.group("host")
    prefix = m_gcr.group("prefix") or ""
    project = m_gcr.group("project")
    image = m_gcr.group("image")
    ar_region = {"": "us", "us": "us", "eu": "europe", "asia": "asia"}[prefix]
    proj_variants = {project, project.replace(":", "/"), project.replace("/", ":")}
    for pv in sorted(proj_variants):
      base_variants.append(f"{host}/{pv}/{image}")
      base_variants.append(f"{ar_region}-docker.pkg.dev/{pv}/{host}/{image}")
      if host == "gcr.io":
        base_variants.append(f"us.gcr.io/{pv}/{image}")
        base_variants.append(f"us-docker.pkg.dev/{pv}/us.gcr.io/{image}")
      elif host == "us.gcr.io":
        base_variants.append(f"gcr.io/{pv}/{image}")
        base_variants.append(f"us-docker.pkg.dev/{pv}/gcr.io/{image}")

  m_ar = AR_REDIRECTED_GCR_RE.match(base_uri)
  if m_ar:
    region = m_ar.group("region")
    project = m_ar.group("project")
    repo = m_ar.group("repo")
    image = m_ar.group("image")
    proj_variants = {project, project.replace(":", "/"), project.replace("/", ":")}
    for pv in sorted(proj_variants):
      base_variants.append(f"{region}-docker.pkg.dev/{pv}/{repo}/{image}")
      base_variants.append(f"{repo}/{pv}/{image}")

  deduped_bases: list[str] = []
  for candidate in base_variants:
    if candidate not in deduped_bases:
      deduped_bases.append(candidate)

  suffixes = [""]
  if explicit_suffix and explicit_suffix not in suffixes:
    suffixes.append(explicit_suffix)
  for digest in extra_digests or []:
    norm_digest = digest if digest.startswith("@") else f"@{digest}"
    if norm_digest not in suffixes:
      suffixes.append(norm_digest)

  expanded: list[str] = []
  for b in deduped_bases:
    for s in suffixes:
      cand = f"{b}{s}"
      if cand not in expanded:
        expanded.append(cand)
  return expanded


def load_vex_config(config_path: Path) -> dict[str, Any]:
  with config_path.open("r", encoding="utf-8") as f:
    return yaml.safe_load(f)


def normalize_statements(
    family_cfg: dict[str, Any], shared_rule_sets: dict[str, Any]
) -> list[dict[str, Any]]:
  """Flattens inherited_rule_sets and per-family statements into individual CVE entries."""
  raw_entries: list[dict[str, Any]] = []
  for rule_set_name in family_cfg.get("inherited_rule_sets", []):
    rule_set = shared_rule_sets.get(rule_set_name, {})
    raw_entries.extend(rule_set.get("statements", []))
  raw_entries.extend(family_cfg.get("statements", []))

  by_cve: dict[str, dict[str, Any]] = {}
  for entry in raw_entries:
    cve_ids = entry.get("ids") or ([entry["id"]] if "id" in entry else [])
    for cve_id in cve_ids:
      by_cve[cve_id] = {
          "id": cve_id,
          "status": entry.get("status", "not_affected"),
          "justification": entry.get(
              "justification", "vulnerable_code_not_in_execute_path"
          ),
          "packages": list(entry.get("packages", [])),
          "impact_statement": entry.get("impact_statement", "").strip(),
          "action_statement": entry.get("action_statement", "").strip(),
      }
  return [by_cve[k] for k in sorted(by_cve.keys())]


def build_openvex_document(
    family_name: str,
    family_cfg: dict[str, Any],
    publisher: dict[str, Any],
    statements: list[dict[str, Any]],
    timestamp: str,
    extra_uris: list[str] | None = None,
    extra_digests: list[str] | None = None,
) -> dict[str, Any]:
  repos = list(family_cfg.get("repositories", []))
  for uri in extra_uris or []:
    if uri not in repos:
      repos.append(uri)

  product_ids: list[str] = []
  for repo in repos:
    for expanded in expand_uri_variants(repo, extra_digests=extra_digests):
      if expanded not in product_ids:
        product_ids.append(expanded)

  openvex_statements: list[dict[str, Any]] = []
  for stmt in statements:
    item: dict[str, Any] = {
        "vulnerability": {"name": stmt["id"]},
        "timestamp": timestamp,
        "products": [{"@id": pid} for pid in product_ids],
        "status": stmt["status"],
    }
    if stmt["status"] == "not_affected":
      item["justification"] = stmt["justification"]
      if stmt.get("impact_statement"):
        item["impact_statement"] = stmt["impact_statement"]
    elif stmt["status"] == "affected" and stmt.get("action_statement"):
      item["action_statement"] = stmt["action_statement"]
    openvex_statements.append(item)

  namespace = publisher.get("namespace", "https://beam.apache.org").rstrip("/")
  return {
      "@context": "https://openvex.dev/ns/v0.2.0",
      "@id": f"{namespace}/vex/{family_name}",
      "author": publisher.get("name", "Apache Beam Project"),
      "role": publisher.get("role", "vendor"),
      "timestamp": timestamp,
      "version": 1,
      "statements": openvex_statements,
  }


def build_csaf_document(
    family_name: str,
    family_cfg: dict[str, Any],
    publisher: dict[str, Any],
    statements: list[dict[str, Any]],
    timestamp: str,
    extra_uris: list[str] | None = None,
    extra_digests: list[str] | None = None,
) -> dict[str, Any]:
  """Builds a CSAF 2.0 VEX document compatible with gcloud artifacts vulnerabilities load-vex."""
  repos = list(family_cfg.get("repositories", []))
  for uri in extra_uris or []:
    if uri not in repos:
      repos.append(uri)

  expanded_uris: list[str] = []
  for repo in repos:
    for variant in expand_uri_variants(repo, extra_digests=extra_digests):
      if variant not in expanded_uris:
        expanded_uris.append(variant)

  branches: list[dict[str, Any]] = []
  product_ids: list[str] = []
  for idx, uri in enumerate(expanded_uris, start=1):
    pid = f"product_{idx}"
    product_ids.append(pid)
    branches.append({
        "category": "product_version",
        "name": uri,
        "product": {
            "name": uri,
            "product_id": pid,
        },
    })

  vulnerabilities: list[dict[str, Any]] = []
  for stmt in statements:
    cve_id = stmt["id"]
    status = stmt["status"]
    csaf_status_key = {
        "not_affected": "known_not_affected",
        "affected": "known_affected",
        "fixed": "fixed",
        "under_investigation": "under_investigation",
    }.get(status, "known_not_affected")

    vuln_entry: dict[str, Any] = {
        "cve": cve_id,
        "title": f"VEX assessment for {cve_id} in {family_name}",
        "notes": [
            {
                "category": "description",
                "title": f"{cve_id} exploitability assessment",
                "text": (
                    stmt.get("impact_statement")
                    or f"{cve_id} is {status} in {family_name}."
                ),
            }
        ],
        "product_status": {
            csaf_status_key: product_ids,
        },
    }
    if status == "not_affected":
      just = CSAF_JUSTIFICATION_MAP.get(
          stmt.get("justification", ""), "vulnerable_code_not_in_execute_path"
      )
      vuln_entry["threats"] = [
          {
              "category": "impact",
              "details": (
                  stmt.get("impact_statement")
                  or f"{cve_id} is not exploitable in {family_name}."
              ),
              "product_ids": product_ids,
          }
      ]
      vuln_entry["flags"] = [
          {
              "label": just,
              "product_ids": product_ids,
          }
      ]
    elif stmt.get("action_statement"):
      vuln_entry["remediations"] = [
          {
              "category": "mitigation",
              "details": stmt["action_statement"],
              "product_ids": product_ids,
          }
      ]
    vulnerabilities.append(vuln_entry)

  return {
      "document": {
          "Category": "csaf_vex",
          "csaf_version": "2.0",
          "title": f"{publisher.get('name', 'Apache Beam')} VEX for {family_name}",
          "publisher": {
              "category": publisher.get("role", "vendor"),
              "name": publisher.get("name", "Apache Beam Project"),
              "namespace": publisher.get("namespace", "https://beam.apache.org"),
              "contact_details": publisher.get(
                  "contact_details",
                  "https://beam.apache.org/community/contact-us/",
              ),
          },
          "tracking": {
              "id": f"apache-beam-vex-{family_name}",
              "status": "final",
              "version": "1",
              "initial_release_date": timestamp,
              "current_release_date": timestamp,
          },
      },
      "product_tree": {
          "branches": branches,
      },
      "vulnerabilities": vulnerabilities,
  }


def build_lichene_rules_snippet(
    config: dict[str, Any],
) -> str:
  """Generates a Lichene Piccolo (`rules.pi`) snippet from the VEX config."""
  shared_sets = config.get("shared_rule_sets", {})
  grouped: dict[tuple[str, str], set[str]] = {}
  for family_name, family_cfg in sorted(
      config.get("image_families", {}).items()
  ):
    for stmt in normalize_statements(family_cfg, shared_sets):
      if stmt["status"] != "not_affected":
        continue
      lichene_status = LICHENE_STATUS_MAP.get(
          stmt["justification"], "CODE_UNREACHABLE"
      )
      key = (lichene_status, stmt["impact_statement"])
      grouped.setdefault(key, set()).add(stmt["id"])

  lines = [
      "# Licensed to the Apache Software Foundation (ASF) under one",
      "# or more contributor license agreements.  See the NOTICE file",
      "# distributed with this work for additional information",
      "# regarding copyright ownership.  The ASF licenses this file",
      "# to you under the Apache License, Version 2.0 (the",
      '# "License"); you may not use this file except in compliance',
      "# with the License.  You may obtain a copy of the License at",
      "#",
      "#   http://www.apache.org/licenses/LICENSE-2.0",
      "#",
      "# Unless required by applicable law or agreed to in writing,",
      "# software distributed under the License is distributed on an",
      '# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY',
      "# KIND, either express or implied.  See the License for the",
      "# specific language governing permissions and limitations",
      "# under the License.",
      "#",
      "# Auto-generated Lichene triage rules from beam_containers_vex.yaml",
      "# Target: //testing/security/lichene/config/env/prod/tombolo_self_service/cloud_dataflow/rules.pi",
      "",
  ]
  for idx, ((lichene_status, justification), cves) in enumerate(
      sorted(grouped.items()), start=1
  ):
    lines.append("containers.triage_vulns(")
    lines.append(f'    rule_id="vex_auto_rule_{idx}_{lichene_status.lower()}",')
    lines.append(f"    triage_as=lichene.TriageStatus.{lichene_status},")
    lines.append(f"    justification={json.dumps(justification)},")
    lines.append('    structured_justifications=["b/502535467", "b/553250543"],')
    lines.append('    approval_process="b/492390417",')
    lines.append("    advisory_references=[")
    for cve in sorted(cves):
      lines.append(f'        "{cve}",')
    lines.append("    ],")
    lines.append("    for_artifacts=[")
    lines.append(
        '        containers.create_ar_filter("google.com:dataflow-containers"),'
    )
    lines.append('        containers.create_gcr_filter("cloud-dataflow"),')
    lines.append(
        '        containers.create_gcr_filter("dataflow-templates-base"),'
    )
    lines.append('        containers.create_gcr_filter("dataflow-templates"),')
    lines.append("    ],")
    lines.append("    applies_also_to_deployments=True,")
    lines.append("),")
  return "\n".join(lines) + "\n"


def parse_artifact_registry_vulns(raw_json: str) -> list[dict[str, Any]]:
  """Parses JSON output from `gcloud artifacts vulnerabilities list --format=json`."""
  if not raw_json.strip():
    return []
  data = json.loads(raw_json)
  if isinstance(data, dict):
    occurrences = (
        data.get("Occurrences")
        or data.get("occurrences")
        or []
    )
  elif isinstance(data, list):
    occurrences = data
  else:
    occurrences = []

  findings: list[dict[str, Any]] = []
  for item in occurrences:
    if not isinstance(item, dict):
      continue
    occ = item.get("occurrence", item)
    if not isinstance(occ, dict):
      continue
    vuln = occ.get("vulnerability", {})
    cve_id = vuln.get("shortDescription", "")
    if not cve_id.startswith("CVE-"):
      continue
    vex_state = (
        vuln.get("vexAssessment", {}).get("state", "")
        if isinstance(vuln.get("vexAssessment"), dict)
        else ""
    )
    raw_pi = vuln.get("packageIssue", [])
    pi_list = [raw_pi] if isinstance(raw_pi, dict) else (raw_pi if isinstance(raw_pi, list) else [])
    for pi in pi_list:
      if not isinstance(pi, dict):
        continue
      findings.append({
          "id": cve_id,
          "package": pi.get("affectedPackage", ""),
          "package_type": pi.get("packageType", "OS"),
          "severity": pi.get(
              "effectiveSeverity", vuln.get("effectiveSeverity", "UNKNOWN")
          ),
          "fix_available": bool(pi.get("fixAvailable", False)),
          "vex_state": vex_state,
          "source": "artifact_registry",
      })
  return findings


def parse_trivy_vulns(raw_json: str) -> list[dict[str, Any]]:
  """Parses JSON output from `trivy image --format json`."""
  if not raw_json.strip():
    return []
  data = json.loads(raw_json)
  findings: list[dict[str, Any]] = []
  for result in data.get("Results", []):
    pkg_type = result.get("Class", "os-pkgs")
    for vuln in result.get("Vulnerabilities", []) or []:
      cve_id = vuln.get("VulnerabilityID", "")
      if not cve_id.startswith("CVE-"):
        continue
      findings.append({
          "id": cve_id,
          "package": vuln.get("PkgName", ""),
          "package_type": pkg_type,
          "severity": vuln.get("Severity", "UNKNOWN"),
          "fix_available": bool(vuln.get("FixedVersion")),
          "vex_state": "",
          "source": "trivy",
      })
  return findings


def parse_govulncheck_json(raw_stream: str) -> set[str]:
  """Extracts reachable Go OSV/CVE IDs reported with symbol-level callstacks by govulncheck."""
  reachable: set[str] = set()
  dec = json.JSONDecoder()
  idx = 0
  text = raw_stream.strip()
  osv_to_cves: dict[str, list[str]] = {}
  called_osvs: set[str] = set()
  while idx < len(text):
    while idx < len(text) and text[idx].isspace():
      idx += 1
    if idx >= len(text):
      break
    obj, idx = dec.raw_decode(text, idx)
    if not isinstance(obj, dict):
      continue
    if "osv" in obj and isinstance(obj["osv"], dict):
      osv_id = obj["osv"].get("id", "")
      aliases = [
          a for a in obj["osv"].get("aliases", []) if a.startswith("CVE-")
      ]
      if osv_id:
        osv_to_cves[osv_id] = aliases
    if "finding" in obj and isinstance(obj["finding"], dict):
      finding = obj["finding"]
      trace = finding.get("trace", [])
      if trace and any(frame.get("function") for frame in trace):
        called_osvs.add(finding.get("osv", ""))
  for osv_id in called_osvs:
    reachable.update(osv_to_cves.get(osv_id, []))
  return reachable


def classify_finding_reachability(
    finding: dict[str, Any],
    reachable_go_cves: set[str] | None = None,
) -> dict[str, Any]:
  """Classifies whether a newly discovered CVE is non-exploitable or requires investigation."""
  pkg = finding.get("package", "")
  cve_id = finding["id"]
  pkg_type = str(finding.get("package_type", "")).upper()

  if pkg in BUILD_ONLY_OS_PACKAGES:
    return {
        "id": cve_id,
        "package": pkg,
        "status": "not_affected",
        "justification": "vulnerable_code_not_in_execute_path",
        "impact_statement": BUILD_ONLY_OS_PACKAGES[pkg],
        "confidence": "high",
    }

  if reachable_go_cves is not None and pkg_type in ("GO", "GOBINARY", "LANG-PKGS"):
    if cve_id not in reachable_go_cves:
      return {
          "id": cve_id,
          "package": pkg,
          "status": "not_affected",
          "justification": "vulnerable_code_not_in_execute_path",
          "impact_statement": (
              f"Verified via govulncheck that {cve_id} in {pkg} has no "
              "reachable symbol call path from the container entrypoint."
          ),
          "confidence": "high",
      }

  return {
      "id": cve_id,
      "package": pkg,
      "status": "under_investigation",
      "justification": "",
      "impact_statement": (
          f"{cve_id} in package {pkg} ({pkg_type}) requires reachability "
          "verification or base image / dependency upgrade."
      ),
      "confidence": "needs_review",
  }


def invoke_llm_triage(
    finding: dict[str, Any],
    family_name: str,
    family_cfg: dict[str, Any],
) -> dict[str, Any] | None:
  """Optionally queries Gemini API when GEMINI_API_KEY is configured for agentic reachability analysis."""
  api_key = os.environ.get("GEMINI_API_KEY", "").strip()
  if not api_key:
    return None
  prompt = (
      "You are a container security reachability analyzer for Apache Beam.\n"
      f"Container family: {family_name}\n"
      f"Runtime entrypoints: {family_cfg.get('runtime_entrypoints', [])}\n"
      f"Vulnerability: {json.dumps(finding)}\n"
      "Return ONLY a JSON object with keys: status ('not_affected' or "
      "'under_investigation'), justification (one of CISA VEX justifications "
      "if not_affected, else ''), and impact_statement (1-2 concise sentences)."
  )
  url = (
      "https://generativelanguage.googleapis.com/v1beta/models/"
      f"gemini-2.5-flash:generateContent?key={api_key}"
  )
  payload = json.dumps({
      "contents": [{"parts": [{"text": prompt}]}],
      "generationConfig": {"responseMimeType": "application/json"},
  }).encode("utf-8")
  req = urllib.request.Request(
      url, data=payload, headers={"Content-Type": "application/json"}
  )
  try:
    with urllib.request.urlopen(req, timeout=30) as resp:
      body = json.loads(resp.read().decode("utf-8"))
      text = body["candidates"][0]["content"]["parts"][0]["text"]
      parsed = json.loads(text)
      if parsed.get("status") in ("not_affected", "under_investigation"):
        return {
            "id": finding["id"],
            "package": finding.get("package", ""),
            "status": parsed["status"],
            "justification": parsed.get(
                "justification", "vulnerable_code_not_in_execute_path"
            ),
            "impact_statement": parsed.get("impact_statement", ""),
            "confidence": "agentic_llm",
        }
  except Exception as exc:  # pylint: disable=broad-except
    print(f"Warning: LLM triage request failed for {finding['id']}: {exc}", file=sys.stderr)
  return None


def cmd_generate(args: argparse.Namespace) -> int:
  config_path = Path(args.config)
  output_dir = Path(args.output_dir)
  config = load_vex_config(config_path)
  publisher = config.get("publisher", {})
  shared_sets = config.get("shared_rule_sets", {})
  timestamp = args.timestamp or datetime.now(timezone.utc).strftime(
      "%Y-%m-%dT%H:%M:%SZ"
  )

  openvex_dir = output_dir / "openvex"
  csaf_dir = output_dir / "csaf"
  lichene_dir = output_dir / "lichene"
  openvex_dir.mkdir(parents=True, exist_ok=True)
  csaf_dir.mkdir(parents=True, exist_ok=True)
  lichene_dir.mkdir(parents=True, exist_ok=True)

  families = config.get("image_families", {})
  selected_families = (
      [args.family] if args.family else sorted(families.keys())
  )
  for family_name in selected_families:
    if family_name not in families:
      raise ValueError(f"Unknown image family: {family_name}")
    family_cfg = families[family_name]
    statements = normalize_statements(family_cfg, shared_sets)

    openvex_doc = build_openvex_document(
        family_name=family_name,
        family_cfg=family_cfg,
        publisher=publisher,
        statements=statements,
        timestamp=timestamp,
        extra_uris=args.extra_uri,
        extra_digests=args.extra_digest,
    )
    csaf_doc = build_csaf_document(
        family_name=family_name,
        family_cfg=family_cfg,
        publisher=publisher,
        statements=statements,
        timestamp=timestamp,
        extra_uris=args.extra_uri,
        extra_digests=args.extra_digest,
    )

    openvex_path = openvex_dir / f"{family_name}.openvex.json"
    csaf_path = csaf_dir / f"{family_name}.vex.json"
    openvex_path.write_text(
        json.dumps(openvex_doc, indent=2) + "\n", encoding="utf-8"
    )
    csaf_path.write_text(
        json.dumps(csaf_doc, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Generated {openvex_path} and {csaf_path} ({len(statements)} CVEs)")

  lichene_path = lichene_dir / "beam_vex_rules.pi"
  lichene_path.write_text(build_lichene_rules_snippet(config), encoding="utf-8")
  print(f"Generated {lichene_path}")
  return 0


def cmd_scan(args: argparse.Namespace) -> int:
  config_path = Path(args.config)
  config = load_vex_config(config_path)
  families = config.get("image_families", {})
  shared_sets = config.get("shared_rule_sets", {})
  if args.family not in families:
    raise ValueError(f"Unknown image family: {args.family}")
  family_cfg = families[args.family]
  covered_cves = {
      s["id"] for s in normalize_statements(family_cfg, shared_sets)
  }

  discovered: list[dict[str, Any]] = []
  if args.ar_image:
    cmd = [
        "gcloud",
        "artifacts",
        "vulnerabilities",
        "list",
        args.ar_image,
        f"--location={args.ar_location}",
        "--format=json",
    ]
    if args.ar_project:
      cmd.append(f"--project={args.ar_project}")
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    discovered.extend(parse_artifact_registry_vulns(res.stdout))

  if args.trivy_image:
    cmd = ["trivy", "image", "--format", "json", "--quiet", args.trivy_image]
    res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    discovered.extend(parse_trivy_vulns(res.stdout))

  reachable_go_cves: set[str] | None = None
  if args.govulncheck_binary:
    cmd = [
        "govulncheck",
        "-format",
        "json",
        "-mode",
        "binary",
        args.govulncheck_binary,
    ]
    res = subprocess.run(cmd, check=False, capture_output=True, text=True)
    reachable_go_cves = parse_govulncheck_json(res.stdout)

  uncovered_by_cve: dict[str, dict[str, Any]] = {}
  for finding in discovered:
    cve_id = finding["id"]
    if cve_id in covered_cves or finding.get("vex_state") == "NOT_AFFECTED":
      continue
    uncovered_by_cve.setdefault(cve_id, finding)

  triage_results: list[dict[str, Any]] = []
  for cve_id in sorted(uncovered_by_cve.keys()):
    finding = uncovered_by_cve[cve_id]
    verdict = classify_finding_reachability(
        finding, reachable_go_cves=reachable_go_cves
    )
    if verdict["status"] == "under_investigation" and args.agentic_triage:
      llm_verdict = invoke_llm_triage(finding, args.family, family_cfg)
      if llm_verdict is not None:
        verdict = llm_verdict
    triage_results.append(verdict)

  report = {
      "family": args.family,
      "ar_image": args.ar_image,
      "trivy_image": args.trivy_image,
      "total_discovered": len(discovered),
      "already_covered_count": len(covered_cves),
      "uncovered_count": len(triage_results),
      "triage_results": triage_results,
  }
  if args.report_json:
    Path(args.report_json).write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

  if args.update_config:
    new_not_affected = [
        r for r in triage_results if r["status"] == "not_affected"
    ]
    if new_not_affected:
      family_stmts = family_cfg.setdefault("statements", [])
      for item in new_not_affected:
        family_stmts.append({
            "id": item["id"],
            "status": "not_affected",
            "justification": item["justification"],
            "packages": [item["package"]] if item.get("package") else [],
            "impact_statement": item["impact_statement"],
        })
      with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)
      print(
          f"Appended {len(new_not_affected)} new not_affected VEX statements to {config_path}"
      )

  print(json.dumps(report, indent=2))
  return 0


def cmd_attach(args: argparse.Namespace) -> int:
  if args.openvex:
    cmd = [
        "cosign",
        "attest",
        "--yes",
        "--type",
        "https://openvex.dev/ns",
        "--predicate",
        args.openvex,
        args.image_uri,
    ]
    print("Running:", " ".join(cmd))
    if not args.dry_run:
      subprocess.run(cmd, check=True)

  if args.csaf:
    cmd = [
        "gcloud",
        "artifacts",
        "vulnerabilities",
        "load-vex",
        f"--uri={args.image_uri}",
        f"--source={args.csaf}",
    ]
    if args.project:
      cmd.append(f"--project={args.project}")
    print("Running:", " ".join(cmd))
    if not args.dry_run:
      subprocess.run(cmd, check=True)

  return 0


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(
      description="Manage OpenVEX and CSAF 2.0 VEX statements for Beam containers."
  )
  subparsers = parser.add_subparsers(dest="subcommand", required=True)

  gen = subparsers.add_parser(
      "generate", help="Generate OpenVEX, CSAF 2.0, and Lichene rules from YAML."
  )
  gen.add_argument(
      "--config",
      default=str(Path(__file__).with_name("beam_containers_vex.yaml")),
      help="Path to canonical VEX YAML file.",
  )
  gen.add_argument(
      "--output-dir",
      default=str(Path(__file__).parent),
      help="Directory to write openvex/, csaf/, and lichene/ outputs.",
  )
  gen.add_argument(
      "--family",
      help="Optional single image family to generate.",
  )
  gen.add_argument(
      "--extra-uri",
      action="append",
      default=[],
      help="Additional image URI(s) to include in generated product trees.",
  )
  gen.add_argument(
      "--extra-digest",
      action="append",
      default=[],
      help="Additional sha256 digest(s) to expand for each repository.",
  )
  gen.add_argument(
      "--timestamp",
      default="2026-11-10T00:00:00Z",
      help="ISO-8601 timestamp for deterministic VEX output.",
  )
  gen.set_defaults(func=cmd_generate)

  scan = subparsers.add_parser(
      "scan",
      help="Scan containers via Artifact Registry, Trivy, and govulncheck.",
  )
  scan.add_argument(
      "--config",
      default=str(Path(__file__).with_name("beam_containers_vex.yaml")),
      help="Path to canonical VEX YAML file.",
  )
  scan.add_argument(
      "--family",
      required=True,
      help="Image family key in the VEX YAML config.",
  )
  scan.add_argument(
      "--ar-image",
      help="Artifact Registry / GCR image URI to query with `gcloud artifacts vulnerabilities list`.",
  )
  scan.add_argument(
      "--ar-location",
      default="us",
      help="Artifact Registry location (default: us).",
  )
  scan.add_argument(
      "--ar-project",
      help="Optional GCP project override for gcloud.",
  )
  scan.add_argument(
      "--trivy-image",
      help="Container image reference to scan with `trivy image --format json`.",
  )
  scan.add_argument(
      "--govulncheck-binary",
      help="Optional compiled Go binary path to analyze with `govulncheck -mode binary`.",
  )
  scan.add_argument(
      "--agentic-triage",
      action="store_true",
      help="Invoke LLM agentic reachability analysis when GEMINI_API_KEY is set.",
  )
  scan.add_argument(
      "--update-config",
      action="store_true",
      help="Append newly verified not_affected CVEs to the VEX YAML config.",
  )
  scan.add_argument(
      "--report-json",
      help="Optional path to write the JSON triage report.",
  )
  scan.set_defaults(func=cmd_scan)

  att = subparsers.add_parser(
      "attach",
      help="Attach OpenVEX via cosign and/or CSAF 2.0 via gcloud load-vex.",
  )
  att.add_argument(
      "--image-uri",
      required=True,
      help="Target container image URI (with @sha256:... for cosign or digest-scoped AR VEX).",
  )
  att.add_argument(
      "--openvex",
      help="Path to OpenVEX JSON predicate file for `cosign attest`.",
  )
  att.add_argument(
      "--csaf",
      help="Path to CSAF 2.0 VEX JSON file for `gcloud artifacts vulnerabilities load-vex`.",
  )
  att.add_argument(
      "--project",
      help="Optional GCP project for `gcloud artifacts vulnerabilities load-vex`.",
  )
  att.add_argument(
      "--dry-run",
      action="store_true",
      help="Print commands without executing.",
  )
  att.set_defaults(func=cmd_attach)

  return parser


def main(argv: list[str] | None = None) -> int:
  parser = build_parser()
  args = parser.parse_args(argv)
  return args.func(args)


if __name__ == "__main__":
  sys.exit(main())
