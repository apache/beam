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

"""Tests for update_ml_dependency_bounds.py. No network access is needed."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import update_ml_dependency_bounds as bounds  # pylint: disable=wrong-import-position
from packaging.version import Version  # pylint: disable=wrong-import-position

SCRIPTS_DIR = Path(__file__).resolve().parent
CONFIG = SCRIPTS_DIR / 'ml_dependency_bounds.yaml'

SETUP_PY = """\
ml_base_core = [
    'onnx>=1.14.1,<2',
    'tensorflow>=2.16.0,<2.22',
    'tokenizers<0.23',
    'torch>=2.3.0,<2.12.0',
    'transformers>=4.28.0,<4.56.0',
    'tokenizers>=0.13.3,<0.22.0',
]
extras = {
    'torch': ['torch>=2.3.0,<2.15.0'],
    'tensorflow': [
        # Comment that must stay.
        'tensorflow>=2.16.0,<2.22',
        'absl-py>=0.12.0'
    ],
    'transformers': [
        'transformers>=4.41.0,!=4.56.*,<5.21.0',
        'tensorflow>=2.12.0',
        'torch>=2.3.0,<2.12.0'
    ],
    'ml_cpu': [
        'tensorflow>=2.12.0',
        'torch==2.8.0+cpu',
        'transformers>=4.28.0,<4.56.0',
    ],
    'onnx': [
        'onnx>=1.16.0,<2',
        'onnxruntime>=1.19.0,<2',
        'skl2onnx>=1.17.0,<2',
        'tensorflow>=2.18.0,<2.22',
        'torch>=2.3.0,<2.12.0',
    ],
}
"""

TOX_INI = """\
[testenv:py311-pytorch-{23,214}]
deps =
  23: torch>=2.3.0,<2.4.0
  214: torch>=2.14.0,<2.15.0
extras = test,gcp

[testenv:py314-pytorch-214]
deps =
  torch>=2.14.0,<2.15.0
extras = test,gcp

[testenv:py311-onnx-{119,latest}]
deps =
  119: onnxruntime>=1.19.0,<1.20
  119: torch>=2.3.0,<2.4.0
  latest: onnxruntime>=1.31.0,<2
  latest: onnx>=1.23.0,<2
  latest: skl2onnx>=1.20.0,<2
  latest: torch>=2.11.0,<2.12.0

[testenv:py311-tensorflow-{216,221}]
deps =
  216: tensorflow>=2.16.0,<2.17
  216: tf-keras>=2.16.0,<2.17
  221: tensorflow>=2.21.0,<2.22
  221: tf-keras>=2.21.0,<2.22
extras =
  216: test
  221: test,gcp

[testenv:py311-transformers-{441,457,latest}]
setenv =
    COMMON_DEPS = 'tensorflow>=2.16,<2.22' tf-keras
    latest: DEPS = 'transformers>=5.20.0,<5.21.0' 'torch>=2.11.0,<2.12.0'
"""

PY311_GRADLE = """\
toxTask "testPy311pytorch-23", "py311-pytorch-23", "${posargs}"
toxTask "testPy311pytorch-214", "py311-pytorch-214", "${posargs}"
toxTask "testPy311onnx-latest", "py311-onnx-latest", "${posargs}"
toxTask "testPy311tensorflow-216", "py311-tensorflow-216", "${posargs}"
toxTask "testPy311tensorflow-221", "py311-tensorflow-221", "${posargs}"
toxTask "testPy311transformers-latest", "py311-transformers-latest", "${posargs}"
"""

PY314_GRADLE = """\
// Keep in sync with [testenv:py314-pytorch-214] in tox.ini.
toxTask "testPy314pytorch-214", "py314-pytorch-214", "${posargs}"
"""

FILES = {
    'sdks/python/setup.py': SETUP_PY,
    'sdks/python/tox.ini': TOX_INI,
    'sdks/python/test-suites/tox/py311/build.gradle': PY311_GRADLE,
    'sdks/python/test-suites/tox/py314/build.gradle': PY314_GRADLE,
}


def fake_pypi(releases: dict[str, list[str]]):
  """Returns a latest_of function backed by fake PyPI JSON payloads."""
  payloads = {
      project: {
          'releases': {
              version: [{
                  'yanked': False
              }]
              for version in versions
          }
      }
      for project, versions in releases.items()
  }
  return lambda project: bounds.latest_stable(payloads[project])


class UpdateMlDependencyBoundsTest(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp_dir.cleanup)
    self.root = Path(self.temp_dir.name)
    for name, text in FILES.items():
      path = self.root / name
      path.parent.mkdir(parents=True, exist_ok=True)
      bounds.write_text(path, text)

  def updater(self, site_ids, releases):
    sites, renames = bounds.load_config(CONFIG)
    selected = [site for site in sites if site.id in site_ids]
    self.assertEqual(len(selected), len(site_ids))
    return bounds.BoundsUpdater(
        self.root, selected, renames, fake_pypi(releases))

  def run_updates(self, site_ids, releases):
    updater = self.updater(site_ids, releases)
    report = updater.run()
    updater.write()
    return report

  def read(self, name):
    return bounds.read_text(self.root / name)

  def test_minor_cap_rises_and_low_factor_stays(self):
    report = self.run_updates(['setup_torch_torch_extra', 'tox_torch_low'],
                              {'torch': ['2.3.0', '2.14.0', '2.15.1']})
    setup_py = self.read('sdks/python/setup.py')
    self.assertIn("'torch': ['torch>=2.3.0,<2.16.0']", setup_py)
    self.assertEqual([(c.old, c.new) for c in report.changes],
                     [('torch>=2.3.0,<2.15.0', 'torch>=2.3.0,<2.16.0')])
    self.assertIn('23: torch>=2.3.0,<2.4.0', self.read('sdks/python/tox.ini'))

  def test_minor_cap_without_new_release_is_unchanged(self):
    report = self.run_updates(['setup_torch_torch_extra'],
                              {'torch': ['2.3.0', '2.14.2']})
    self.assertEqual(report.changes, [])
    self.assertEqual(report.holds, [])
    self.assertEqual(self.read('sdks/python/setup.py'), SETUP_PY)

  def test_site_at_max_exclusive_holds_and_reports_reason(self):
    report = self.run_updates(['setup_torch_ml_base_core'],
                              {'torch': ['2.14.0', '2.15.1']})
    self.assertEqual(self.read('sdks/python/setup.py'), SETUP_PY)
    self.assertEqual(report.changes, [])
    self.assertEqual([h.site for h in report.holds],
                     ['setup_torch_ml_base_core'])
    self.assertIn('triton', report.holds[0].reason)
    self.assertIn(report.holds[0].reason, bounds.render_pr_body(report))

  def test_major_cap_stays_when_latest_is_inside(self):
    report = self.run_updates(['setup_onnx_ml_base_core'], {'onnx': ['1.23.0']})
    self.assertEqual(self.read('sdks/python/setup.py'), SETUP_PY)
    self.assertEqual(report.holds, [])

  def test_major_cap_stays_and_holds_when_latest_is_next_major(self):
    report = self.run_updates(['setup_onnx_ml_base_core'],
                              {'onnx': ['1.23.0', '2.0.0']})
    self.assertEqual(self.read('sdks/python/setup.py'), SETUP_PY)
    self.assertEqual(report.changes, [])
    self.assertEqual([h.site for h in report.holds],
                     ['setup_onnx_ml_base_core'])

  def test_transformers_exclusion_survives(self):
    self.run_updates(['setup_transformers_extra'],
                     {'transformers': ['4.57.0', '5.23.1']})
    self.assertIn(
        "'transformers>=4.41.0,!=4.56.*,<5.24.0'",
        self.read('sdks/python/setup.py'))

  def test_transformers_base_core_crosses_major(self):
    self.run_updates(['setup_transformers_ml_base_core'],
                     {'transformers': ['4.57.0', '5.23.1']})
    setup_py = self.read('sdks/python/setup.py')
    self.assertIn("'transformers>=4.28.0,<5.24.0'", setup_py)
    # ml_cpu is not a site and keeps its bound.
    self.assertIn("'transformers>=4.28.0,<4.56.0'", setup_py)

  def test_both_tokenizers_lines_survive(self):
    report = self.run_updates(
        ['setup_tokenizers_cap', 'setup_tokenizers_range'],
        {'tokenizers': ['0.22.1', '0.23.1']})
    setup_py = self.read('sdks/python/setup.py')
    self.assertEqual(setup_py, SETUP_PY)
    self.assertEqual([h.site for h in report.holds],
                     ['setup_tokenizers_cap', 'setup_tokenizers_range'])

  def test_cpu_pin_survives(self):
    self.run_updates(['setup_torch_torch_extra', 'setup_torch_onnx_extra'],
                     {'torch': ['2.15.1']})
    self.assertIn("'torch==2.8.0+cpu'", self.read('sdks/python/setup.py'))

  def test_site_below_ceiling_stops_at_ceiling(self):
    sites, renames = bounds.load_config(CONFIG)
    selected = [s for s in sites if s.id == 'setup_torch_torch_extra']
    selected[0].max_exclusive = '2.16.0'
    updater = bounds.BoundsUpdater(
        self.root, selected, renames, fake_pypi({'torch': ['2.17.0']}))
    report = updater.run()
    self.assertEqual([c.new for c in report.changes], ['torch>=2.3.0,<2.16.0'])
    self.assertEqual(len(report.holds), 1)

  def test_pytorch_high_factor_and_tasks_rename_together(self):
    report = self.run_updates([
        'setup_torch_torch_extra',
        'tox_torch_low',
        'tox_torch_high',
        'tox_torch_py314'
    ], {'torch': ['2.15.1']})
    tox_ini = self.read('sdks/python/tox.ini')
    self.assertIn('[testenv:py311-pytorch-{23,215}]', tox_ini)
    self.assertIn('  23: torch>=2.3.0,<2.4.0', tox_ini)
    self.assertIn('  215: torch>=2.15.0,<2.16.0', tox_ini)
    self.assertIn('[testenv:py314-pytorch-215]', tox_ini)
    self.assertIn('deps =\n  torch>=2.15.0,<2.16.0\n', tox_ini)
    self.assertNotIn('214', tox_ini)
    py311 = self.read('sdks/python/test-suites/tox/py311/build.gradle')
    self.assertIn('"testPy311pytorch-215", "py311-pytorch-215"', py311)
    self.assertIn('"testPy311pytorch-23", "py311-pytorch-23"', py311)
    self.assertNotIn('pytorch-214', py311)
    py314 = self.read('sdks/python/test-suites/tox/py314/build.gradle')
    self.assertIn('"testPy314pytorch-215", "py314-pytorch-215"', py314)
    self.assertIn('[testenv:py314-pytorch-215]', py314)
    self.assertEqual([(r.group, r.old, r.new) for r in report.renames],
                     [('pytorch', '214', '215')])

  def test_tensorflow_sites_move_together(self):
    ids = [
        'setup_tensorflow_ml_base_core',
        'setup_tensorflow_extra',
        'setup_tensorflow_onnx_extra',
        'tox_tensorflow_common_deps',
        'tox_tensorflow_low',
        'tox_tensorflow_high',
        'tox_tf_keras_high'
    ]
    self.run_updates(ids, {'tensorflow': ['2.21.0', '2.22.1']})
    setup_py = self.read('sdks/python/setup.py')
    self.assertEqual(setup_py.count('<2.23'), 3)
    self.assertNotIn('<2.22', setup_py)
    self.assertIn('# Comment that must stay.', setup_py)
    self.assertIn("'tensorflow>=2.12.0'", setup_py)
    tox_ini = self.read('sdks/python/tox.ini')
    self.assertIn('[testenv:py311-tensorflow-{216,222}]', tox_ini)
    self.assertIn('  216: tensorflow>=2.16.0,<2.17', tox_ini)
    self.assertIn('  222: tensorflow>=2.22.0,<2.23', tox_ini)
    self.assertIn('  222: tf-keras>=2.22.0,<2.23', tox_ini)
    self.assertIn('  222: test,gcp', tox_ini)
    self.assertIn("COMMON_DEPS = 'tensorflow>=2.16,<2.23'", tox_ini)
    py311 = self.read('sdks/python/test-suites/tox/py311/build.gradle')
    self.assertIn('"testPy311tensorflow-222", "py311-tensorflow-222"', py311)
    self.assertIn('"testPy311tensorflow-216", "py311-tensorflow-216"', py311)

  def test_transformers_latest_window_follows_extra(self):
    self.run_updates(['setup_transformers_extra', 'tox_transformers_latest'],
                     {'transformers': ['5.23.1']})
    tox_ini = self.read('sdks/python/tox.ini')
    self.assertIn(
        "latest: DEPS = 'transformers>=5.23.0,<5.24.0' "
        "'torch>=2.11.0,<2.12.0'",
        tox_ini)
    self.assertIn(
        '"testPy311transformers-latest", "py311-transformers-latest"',
        self.read('sdks/python/test-suites/tox/py311/build.gradle'))

  def test_onnx_latest_floor_rises_and_major_cap_stays(self):
    self.run_updates(['tox_onnxruntime_latest', 'tox_onnx_latest'], {
        'onnxruntime': ['1.32.1'], 'onnx': ['1.23.0']
    })
    tox_ini = self.read('sdks/python/tox.ini')
    self.assertIn('latest: onnxruntime>=1.32.0,<2', tox_ini)
    # The onnx floor is already the newest minor.
    self.assertIn('latest: onnx>=1.23.0,<2', tox_ini)
    self.assertIn('119: onnxruntime>=1.19.0,<1.20', tox_ini)

  def test_second_run_is_noop(self):
    ids = [
        'setup_torch_ml_base_core',
        'setup_torch_torch_extra',
        'setup_torch_transformers_extra',
        'setup_torch_onnx_extra',
        'tox_torch_low',
        'tox_torch_high',
        'tox_torch_py314',
        'setup_tensorflow_ml_base_core',
        'setup_tensorflow_extra',
        'setup_tensorflow_onnx_extra',
        'tox_tensorflow_common_deps',
        'tox_tensorflow_low',
        'tox_tensorflow_high',
        'tox_tf_keras_high',
        'setup_transformers_ml_base_core',
        'setup_transformers_extra',
        'tox_transformers_latest',
        'setup_tokenizers_cap',
        'setup_tokenizers_range',
        'setup_onnx_ml_base_core',
        'setup_onnx_extra',
        'setup_onnxruntime_extra',
        'setup_skl2onnx_extra',
        'tox_onnxruntime_latest',
        'tox_onnx_latest',
        'tox_skl2onnx_latest'
    ]
    releases = {
        'torch': ['2.15.1'],
        'tensorflow': ['2.22.1'],
        'transformers': ['5.23.1'],
        'tokenizers': ['0.23.1'],
        'onnx': ['1.24.0'],
        'onnxruntime': ['1.32.1'],
        'skl2onnx': ['1.21.0'],
    }
    first = self.run_updates(ids, releases)
    self.assertTrue(first.changes)
    after_first = {name: self.read(name) for name in FILES}
    second = self.run_updates(ids, releases)
    self.assertEqual(second.changes, [])
    self.assertEqual(second.renames, [])
    self.assertEqual({name: self.read(name) for name in FILES}, after_first)

  def test_line_endings_are_preserved(self):
    bounds.write_text(
        self.root / 'sdks/python/setup.py', SETUP_PY.replace('\n', '\r\n'))
    self.run_updates(['setup_torch_torch_extra'], {'torch': ['2.15.1']})
    setup_py = self.read('sdks/python/setup.py')
    self.assertIn("'torch>=2.3.0,<2.16.0'", setup_py)
    self.assertEqual(setup_py.count('\n'), setup_py.count('\r\n'))

  def test_latest_stable_ignores_unstable_and_yanked(self):
    payload = {
        'releases': {
            '2.0.0': [{
                'yanked': False
            }],
            '2.1.0': [{
                'yanked': True
            }],
            '2.2.0rc1': [{
                'yanked': False
            }],
            '2.2.0.dev1': [{
                'yanked': False
            }],
            '2.2.0+cpu': [{
                'yanked': False
            }],
            '2.3.0': [],
        }
    }
    self.assertEqual(bounds.latest_stable(payload), Version('2.0.0'))

  def test_pr_body_lists_changes_holds_and_review_notes(self):
    report = self.run_updates(
        ['setup_torch_ml_base_core', 'setup_torch_torch_extra'],
        {'torch': ['2.15.1']})
    body = bounds.render_pr_body(report)
    self.assertIn('close and reopen the PR', body)
    self.assertIn('Do not merge this PR while tests are failing.', body)
    self.assertIn('torch>=2.3.0,<2.15.0 -> torch>=2.3.0,<2.16.0', body)
    self.assertIn('setup_torch_ml_base_core:', body)
    self.assertIn('tokenizers stays below 0.22', body)

  def test_config_matches_repository_files(self):
    sites, renames = bounds.load_config(CONFIG)
    updater = bounds.BoundsUpdater(
        SCRIPTS_DIR.parents[1],
        sites,
        renames, lambda project: Version('0.0.1'))
    # Every site must resolve in the real files. No file is written.
    updater.run()


if __name__ == '__main__':
  unittest.main()
