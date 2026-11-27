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

"""Raises the exclusive upper bounds of ML dependencies.

For every site listed in ml_dependency_bounds.yaml the script reads the current
requirement from the repository files, compares it with the latest stable
release on PyPI, and rewrites only the bound token that has to move. Tox
environments and Gradle tasks that carry the version of the highest tested
window are renamed together with the window.

Run it from anywhere:

  python scripts/tools/update_ml_dependency_bounds.py
"""

import argparse
import http.client
import json
import re
import sys
import time
import urllib.request
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Callable
from typing import Optional

import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion
from packaging.version import Version

PR_TITLE = 'Update ML dependency bounds'

# Same wording as the PR created by update_python_dependencies.yml.
PR_REVIEW_SENTENCE = (
    'This PR was created by automation - by default this does not execute '
    'tests due to a limitation in GitHub Actions. To review this PR, please '
    '(1) review the contents to make sure all dependency upgrades are '
    'formatted correctly, (2) close and reopen the PR (3) Approve/merge if '
    'tests pass successfully.')

TOKENIZERS_NOTE = (
    'tokenizers stays below 0.22 in the same requirement list as '
    'transformers in ml_base_core. The two can conflict. If a test fails '
    'for that reason, hold transformers instead of raising tokenizers.')


@dataclass
class Site:
  id: str
  file: str
  package: str
  pypi: str
  pattern: str
  role: str
  occurrence: int = 1
  max_exclusive: Optional[str] = None
  hold_reason: Optional[str] = None
  follow: Optional[str] = None
  floor: bool = False
  rename: Optional[str] = None


@dataclass
class Change:
  site: str
  file: str
  old: str
  new: str


@dataclass
class Hold:
  site: str
  reason: str


@dataclass
class Rename:
  group: str
  old: str
  new: str


@dataclass
class Report:
  changes: list[Change] = field(default_factory=list)
  holds: list[Hold] = field(default_factory=list)
  renames: list[Rename] = field(default_factory=list)


def latest_stable(payload: dict) -> Version:
  """Returns the newest release that is not yanked, pre, dev or local."""
  latest = None
  for text, files in payload.get('releases', {}).items():
    if not files or any(entry.get('yanked') for entry in files):
      continue
    try:
      version = Version(text)
    except InvalidVersion:
      continue
    if (version.is_prerelease or version.is_devrelease or
        version.local is not None):
      continue
    if latest is None or version > latest:
      latest = version
  if latest is None:
    raise RuntimeError('No stable release found.')
  return latest


def fetch_latest_stable(project: str, attempts: int = 3) -> Version:
  url = f'https://pypi.org/pypi/{project}/json'
  for attempt in range(1, attempts + 1):
    try:
      with urllib.request.urlopen(url, timeout=60) as response:
        return latest_stable(json.load(response))
    except (OSError, http.client.HTTPException):
      # Large PyPI responses are occasionally cut off.
      if attempt == attempts:
        raise
      time.sleep(5 * attempt)


def exclusive_upper(requirement: Requirement) -> Optional[Version]:
  for spec in requirement.specifier:
    if spec.operator == '<':
      return Version(spec.version)
  return None


def lower_bound(requirement: Requirement) -> Optional[Version]:
  for spec in requirement.specifier:
    if spec.operator == '>=':
      return Version(spec.version)
  return None


def is_major_cap(upper: Version) -> bool:
  """True for caps such as <2 or <4.0.0."""
  return all(part == 0 for part in upper.release[1:])


def format_minor(major: int, minor: int, template: Version) -> str:
  """Formats major.minor with as many parts as the template has."""
  if len(template.release) >= 3:
    return f'{major}.{minor}.0'
  return f'{major}.{minor}'


def replace_bound(
    requirement_text: str, operator: str, new_version: str) -> str:
  """Replaces the version of one specifier and keeps the rest of the text."""
  for spec in Requirement(requirement_text).specifier:
    if spec.operator == operator:
      token = re.compile(
          r'(?<![<>=!~])' + re.escape(operator + spec.version) +
          r'(?![0-9A-Za-z.*+])')
      return token.sub(
          lambda _: operator + new_version, requirement_text, count=1)
  raise RuntimeError(f'No {operator} specifier in {requirement_text}')


def factor_of(requirement_text: str) -> str:
  """Returns the tox factor for the window, for example 214 for >=2.14.0."""
  lower = lower_bound(Requirement(requirement_text))
  return f'{lower.major}{lower.minor}'


def rename_factor(text: str, env_prefix: str, old: str, new: str) -> str:
  """Renames tox env names, factor lines and Gradle task names."""
  # Factor-prefixed lines, such as "214: torch>=...", inside the tox sections
  # of this package.
  header = re.compile(r'\[testenv:[^\]\r\n]*' + re.escape(env_prefix) + '-')
  factor_line = re.compile(r'(?m)^([ \t]+)' + old + ':')
  sections = re.split(r'(?m)^(?=\[)', text)
  sections = [
      factor_line.sub(r'\g<1>' + new +
                      ':', section) if header.match(section) else section
      for section in sections
  ]
  text = ''.join(sections)
  # Brace lists such as py311-pytorch-{23,214}.
  text = re.sub(
      r'(' + re.escape(env_prefix) + r'-\{[0-9,]*?)\b' + old + r'\b',
      r'\g<1>' + new,
      text)
  # Env and task names such as py311-pytorch-214 and testPy311pytorch-214.
  return re.sub(
      r'(' + re.escape(env_prefix) + r'-)' + old + r'\b', r'\g<1>' + new, text)


def read_text(path: Path) -> str:
  # newline='' keeps the line endings of the file.
  with open(path, encoding='utf-8', newline='') as handle:
    return handle.read()


def write_text(path: Path, text: str) -> None:
  with open(path, 'w', encoding='utf-8', newline='') as handle:
    handle.write(text)


def load_config(path: Path) -> tuple[list[Site], dict[str, dict]]:
  with open(path, encoding='utf-8') as handle:
    config = yaml.safe_load(handle)
  sites = [Site(**entry) for entry in config['sites']]
  seen: set[str] = set()
  for site in sites:
    if site.id in seen:
      raise ValueError(f'Duplicate site id {site.id}')
    if site.role not in ('high', 'low'):
      raise ValueError(f'{site.id}: role must be high or low')
    if 'req' not in re.compile(site.pattern).groupindex:
      raise ValueError(f'{site.id}: pattern needs a named group "req"')
    if site.follow and site.follow not in seen:
      raise ValueError(f'{site.id}: follow must name an earlier site')
    seen.add(site.id)
  return sites, config.get('renames', {})


class BoundsUpdater:
  def __init__(
      self,
      repo_root: Path,
      sites: list[Site],
      renames: dict[str, dict],
      latest_of: Callable[[str], Version] = fetch_latest_stable):
    self.repo_root = repo_root
    self.sites = sites
    self.renames = renames
    self.latest_of = latest_of
    self.texts: dict[str, str] = {}
    self.original: dict[str, str] = {}
    self.uppers: dict[str, Optional[Version]] = {}
    self.pending_renames: list[Rename] = []
    self.report = Report()
    self._latest_cache: dict[str, Version] = {}

  def run(self) -> Report:
    files = {site.file for site in self.sites}
    for spec in self.renames.values():
      files.update(spec['files'])
    for name in sorted(files):
      self.texts[name] = read_text(self.repo_root / name)
    self.original = dict(self.texts)
    for site in self.sites:
      self._process(site)
    for rename in self.pending_renames:
      spec = self.renames[rename.group]
      for name in spec['files']:
        self.texts[name] = rename_factor(
            self.texts[name], spec['env_prefix'], rename.old, rename.new)
      self.report.renames.append(rename)
    return self.report

  def write(self) -> None:
    for name, text in self.texts.items():
      if text != self.original[name]:
        write_text(self.repo_root / name, text)

  def _latest(self, project: str) -> Version:
    if project not in self._latest_cache:
      self._latest_cache[project] = self.latest_of(project)
    return self._latest_cache[project]

  def _hold(self, site: Site, default_reason: str) -> None:
    self.report.holds.append(Hold(site.id, site.hold_reason or default_reason))

  def _process(self, site: Site) -> None:
    text = self.texts[site.file]
    matches = list(re.finditer(site.pattern, text, re.MULTILINE))
    if len(matches) < site.occurrence:
      raise RuntimeError(
          f'{site.id}: occurrence {site.occurrence} of the pattern was not '
          f'found in {site.file}')
    match = matches[site.occurrence - 1]
    old = match.group('req')
    requirement = Requirement(old)
    if canonicalize_name(requirement.name) != canonicalize_name(site.package):
      raise RuntimeError(f'{site.id}: matched {old}, expected {site.package}')
    upper = exclusive_upper(requirement)
    self.uppers[site.id] = upper
    if site.role == 'low':
      return
    if upper is None:
      raise RuntimeError(f'{site.id}: {old} has no exclusive upper bound')

    if site.follow:
      new = self._follow(site, old, upper)
    elif site.floor:
      new = self._raise_floor(site, old, requirement, upper)
    else:
      new = self._raise_upper(site, old, upper)
    if new is None or new == old:
      return

    self.texts[site.file] = (
        text[:match.start('req')] + new + text[match.end('req'):])
    self.uppers[site.id] = exclusive_upper(Requirement(new))
    self.report.changes.append(Change(site.id, site.file, old, new))
    if site.rename and factor_of(old) != factor_of(new):
      rename = Rename(site.rename, factor_of(old), factor_of(new))
      if rename not in self.pending_renames:
        self.pending_renames.append(rename)

  def _raise_upper(self, site: Site, old: str, upper: Version) -> Optional[str]:
    latest = self._latest(site.pypi)
    if latest < upper:
      return None
    if is_major_cap(upper):
      self._hold(
          site, f'{site.pypi} {latest} is outside the major cap <{upper}.')
      return None
    ceiling = Version(site.max_exclusive) if site.max_exclusive else None
    if ceiling is not None and upper >= ceiling:
      self._hold(site, f'{site.pypi} {latest} is held below {ceiling}.')
      return None
    new_upper = Version(f'{latest.major}.{latest.minor + 1}')
    if ceiling is not None and new_upper > ceiling:
      new_upper = ceiling
      self._hold(site, f'{site.pypi} {latest} is held below {ceiling}.')
    return replace_bound(
        old, '<', format_minor(new_upper.major, new_upper.minor, upper))

  def _follow(self, site: Site, old: str, upper: Version) -> str:
    """Returns the newest minor window inside the followed upper bound."""
    anchor = self.uppers[site.follow]
    if anchor is None or anchor.minor == 0:
      raise RuntimeError(
          f'{site.id}: {site.follow} has no minor upper bound to follow')
    new = replace_bound(old, '>=', f'{anchor.major}.{anchor.minor - 1}.0')
    return replace_bound(
        new, '<', format_minor(anchor.major, anchor.minor, upper))

  def _raise_floor(
      self, site: Site, old: str, requirement: Requirement,
      upper: Version) -> Optional[str]:
    latest = self._latest(site.pypi)
    if latest >= upper:
      return None
    floor = Version(f'{latest.major}.{latest.minor}.0')
    if floor <= lower_bound(requirement):
      return None
    return replace_bound(old, '>=', str(floor))


def render_pr_body(report: Report) -> str:
  lines = [PR_REVIEW_SENTENCE, '']
  lines.append(
      'Review is required. Do not merge this PR while tests are failing.')
  lines.append('')
  lines.append('Bumped bounds:')
  for change in report.changes:
    lines.append(
        f'- {change.site} ({change.file}): {change.old} -> {change.new}')
  for rename in report.renames:
    lines.append(
        f'- renamed {rename.group} factor {rename.old} -> {rename.new}')
  lines.append('')
  lines.append('Held:')
  if report.holds:
    for hold in report.holds:
      lines.append(f'- {hold.site}: {hold.reason}')
  else:
    lines.append('- none')
  lines.append('')
  lines.append(TOKENIZERS_NOTE)
  return '\n'.join(lines) + '\n'


def main(argv: Optional[list[str]] = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
  scripts_dir = Path(__file__).resolve().parent
  parser.add_argument(
      '--repo-root',
      type=Path,
      default=scripts_dir.parents[1],
      help='Root of the Beam repository.')
  parser.add_argument(
      '--config', type=Path, default=scripts_dir / 'ml_dependency_bounds.yaml')
  parser.add_argument(
      '--dry-run', action='store_true', help='Report without editing files.')
  parser.add_argument(
      '--pr-body-file',
      type=Path,
      help='Write the pull request body to this file.')
  args = parser.parse_args(argv)

  sites, renames = load_config(args.config)
  updater = BoundsUpdater(args.repo_root, sites, renames)
  report = updater.run()
  if not args.dry_run:
    updater.write()
  if args.pr_body_file:
    args.pr_body_file.write_text(render_pr_body(report), encoding='utf-8')
  for change in report.changes:
    print(f'CHANGE {change.site}: {change.old} -> {change.new}')
  for rename in report.renames:
    print(f'RENAME {rename.group}: {rename.old} -> {rename.new}')
  for hold in report.holds:
    print(f'HOLD {hold.site}: {hold.reason}')
  return 0


if __name__ == '__main__':
  sys.exit(main())
