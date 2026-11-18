#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

set -euo pipefail

WORKSPACE="${WORKSPACE:-$(pwd)}"
BUCKET_NAME="beam-python-nightly-snapshots"
BUCKET="gs://${BUCKET_NAME}"
HTTPS_BUCKET_URL="https://storage.googleapis.com/${BUCKET_NAME}"

VERSION="$(awk -F"['\"]" '/^__version__/{print $2}' "${WORKSPACE}/sdks/python/apache_beam/version.py")"
if [[ -z "${VERSION}" ]]; then
  echo "Failed to determine VERSION from sdks/python/apache_beam/version.py" >&2
  exit 1
fi

read_python_versions() {
  local line
  line="$(grep -E '^python_versions=' "${WORKSPACE}/gradle.properties" || true)"
  if [[ -z "${line}" ]]; then
    echo "Could not find python_versions= in ${WORKSPACE}/gradle.properties" >&2
    exit 1
  fi
  local raw="${line#python_versions=}"
  raw="${raw//$'\r'/}"
  IFS=',' read -ra PY_VERSIONS <<< "${raw}"
  if (( ${#PY_VERSIONS[@]} == 0 )); then
    echo "Empty python_versions in ${WORKSPACE}/gradle.properties" >&2
    exit 1
  fi
}

upload_latest_url_list() {
  local urls_content="$1"
  local tmp_latest
  tmp_latest="$(mktemp)"
  printf '%s\n' "${urls_content}" > "${tmp_latest}"
  gcloud storage cp \
    --content-type="text/plain; charset=utf-8" \
    --cache-control="no-cache, max-age=0" \
    "${tmp_latest}" "${BUCKET}/${VERSION}/latest"
  rm -f "${tmp_latest}"
}

publish_snapshot() {
  local time
  time="$(date +"%Y-%m-%dT%H:%M:%S")"
  local snapshot="apache-beam-${VERSION}-${time}.tar.gz"
  local dep_snapshot_root="${BUCKET}/dependency_requirements_snapshot"
  local dep_snapshot_file_name="beam-py-requirements-${time}.txt"

  # Snapshots are built by Gradle task :sdks:python:depSnapshot
  # and located under Gradle build directory.
  cd "${WORKSPACE}/sdks/python/build"

  # Rename the file to be apache-beam-{VERSION}-{datetime}.tar.gz
  # Notice that the distribution name of beam can be "apache-beam" with
  # setuptools<69.3.0 or "apache_beam" with setuptools>=69.3.0.
  shopt -s nullglob
  local tarballs=(apache[-_]beam-"${VERSION}"*.tar.gz)
  if (( ${#tarballs[@]} == 0 )); then
    echo "No snapshot tarball matching apache[-_]beam-${VERSION}*.tar.gz found in $(pwd)" >&2
    exit 1
  fi
  for file in "${tarballs[@]}"; do
    mv "${file}" "${snapshot}"
  done

  # Upload to gcs bucket.
  gcloud storage cp "${snapshot}" "${BUCKET}/${VERSION}/"

  # Upload requirements.txt to gcs.
  gcloud storage cp requirements.txt "${dep_snapshot_root}/${dep_snapshot_file_name}"
}

postrelease_check() {
  if [[ -n "${release:-}" && "${VERSION}" != "${release}" ]]; then
    echo "Version mismatch: version.py has '${VERSION}', expected release '${release}'" >&2
    exit 1
  fi
  read_python_versions
  local commit_sha
  commit_sha="$(git -C "${WORKSPACE}" rev-parse HEAD)"

  local build_ids=()
  for v in "${PY_VERSIONS[@]}"; do
    v="$(echo "${v}" | tr -d '[:space:]')"
    build_ids+=("cp${v//./}-manylinux_x86_64")
  done

  local existing_uris
  existing_uris="$(gcloud storage ls "${BUCKET}/${VERSION}/${commit_sha}/*.whl" 2>/dev/null || true)"

  local ver_escaped="${VERSION//./\\.}"
  local all_present=true
  for v in "${PY_VERSIONS[@]}"; do
    v="$(echo "${v}" | tr -d '[:space:]')"
    local cp_tag="cp${v//./}"
    if ! grep -Eq "/apache[-_]beam-${ver_escaped}-${cp_tag}-.*manylinux.*_x86_64\.whl$" <<< "${existing_uris}"; then
      all_present=false
      break
    fi
  done

  if [[ "${all_present}" == "true" ]]; then
    echo "All ${#PY_VERSIONS[@]} wheels already published at ${BUCKET}/${VERSION}/${commit_sha}/; skipping wheel build."
    local latest_content=""
    latest_content="$(gcloud storage cat "${BUCKET}/${VERSION}/latest" 2>/dev/null || true)"
    if ! grep -Fq "/${VERSION}/${commit_sha}/" <<< "${latest_content}"; then
      echo "Updating ${BUCKET}/${VERSION}/latest to point to ${commit_sha}."
      local https_urls
      https_urls="$(sed "s|^${BUCKET}/|${HTTPS_BUCKET_URL}/|" <<< "${existing_uris}" | sort)"
      upload_latest_url_list "${https_urls}"
    fi
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
      echo "already_published=true" >> "${GITHUB_OUTPUT}"
    fi
  else
    echo "Wheels not yet published for ${VERSION} @ ${commit_sha}; will build: ${build_ids[*]}"
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
      {
        echo "already_published=false"
        echo "build_identifiers=${build_ids[*]}"
      } >> "${GITHUB_OUTPUT}"
    fi
  fi
}

postrelease_prepare_sdist() {
  local output_dir="${1:?Missing required <output_dir> argument for postrelease-prepare-sdist}"
  rm -rf "${WORKSPACE}/sdks/python/dist" "${output_dir}"
  python -m pip install -U build
  (cd "${WORKSPACE}/sdks/python" && python -m build --sdist)
  shopt -s nullglob
  local sdist_files=("${WORKSPACE}"/sdks/python/dist/*.tar.gz)
  if (( ${#sdist_files[@]} != 1 )); then
    echo "Expected exactly 1 sdist tarball, found: ${sdist_files[*]}" >&2
    exit 1
  fi
  mkdir -p "${output_dir}"
  tar -xzf "${sdist_files[0]}" --strip-components=1 -C "${output_dir}"
}

postrelease_publish() {
  local wheelhouse_dir="${1:?Missing required <wheelhouse_dir> argument for postrelease-publish}"
  read_python_versions
  local commit_sha
  commit_sha="$(git -C "${WORKSPACE}" rev-parse HEAD)"
  local dest_prefix="${BUCKET}/${VERSION}/${commit_sha}"

  shopt -s nullglob
  local whl_files=()
  local https_urls=()
  for v in "${PY_VERSIONS[@]}"; do
    v="$(echo "${v}" | tr -d '[:space:]')"
    local cp_tag="cp${v//./}"
    local matches=("${wheelhouse_dir}"/apache[-_]beam-"${VERSION}"-"${cp_tag}"-*-manylinux*_x86_64.whl)
    if (( ${#matches[@]} != 1 )); then
      echo "Expected exactly 1 wheel for Python ${v} (${cp_tag}) in ${wheelhouse_dir}, found: ${matches[*]}" >&2
      exit 1
    fi
    whl_files+=("${matches[0]}")
    https_urls+=("${HTTPS_BUCKET_URL}/${VERSION}/${commit_sha}/$(basename "${matches[0]}")")
  done

  # 1. Upload immutable wheels first.
  gcloud storage cp \
    --cache-control="public, max-age=31536000, immutable" \
    "${whl_files[@]}" "${dest_prefix}/"

  # 2. Upload plain-text URL list to <release>/latest last.
  local urls_text
  urls_text="$(printf '%s\n' "${https_urls[@]}")"
  upload_latest_url_list "${urls_text}"

  echo "Published post-release wheels (${VERSION} @ ${commit_sha}):"
  echo "${urls_text}"

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### Published Python Post-Release Wheels (${VERSION})"
      echo "- **Latest pointer:** \`${HTTPS_BUCKET_URL}/${VERSION}/latest\`"
      echo "- **Commit SHA:** \`${commit_sha}\`"
      echo '```text'
      echo "${urls_text}"
      echo '```'
    } >> "${GITHUB_STEP_SUMMARY}"
  fi
}

usage() {
  echo "Usage: $0 <snapshot|postrelease-check|postrelease-prepare-sdist <output_dir>|postrelease-publish <wheelhouse_dir>>" >&2
}

if (( $# < 1 )); then
  usage
  exit 1
fi

MODE="$1"
case "${MODE}" in
  snapshot)
    publish_snapshot
    ;;
  postrelease-check)
    postrelease_check
    ;;
  postrelease-prepare-sdist)
    if (( $# != 2 )) || [[ -z "$2" ]]; then
      usage
      exit 1
    fi
    postrelease_prepare_sdist "$2"
    ;;
  postrelease-publish)
    if (( $# != 2 )) || [[ -z "$2" ]]; then
      usage
      exit 1
    fi
    postrelease_publish "$2"
    ;;
  *)
    echo "Unknown subcommand: ${MODE}" >&2
    usage
    exit 1
    ;;
esac
