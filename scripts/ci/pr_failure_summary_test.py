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
"""Unit tests for scripts/ci/pr_failure_summary.py."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import pr_failure_summary as pfs  # noqa: E402


class PrFailureSummaryTest(unittest.TestCase):

  def test_dedupes_reruns_and_excludes_bot_checks(self):
    checks = [
        {
            'id': 10,
            'name': 'PreCommit Java',
            'status': 'completed',
            'conclusion': 'failure',
            'completed_at': '2026-11-10T10:00:00Z',
        },
        {
            'id': 20,
            'name': 'PreCommit Java',
            'status': 'completed',
            'conclusion': 'success',
            'completed_at': '2026-11-10T10:20:00Z',
        },
        {
            'id': 30,
            'name': 'codecov/patch',
            'status': 'in_progress',
            'conclusion': None,
        },
        {
            'id': 31,
            'name': 'Summarize PR Check Failures',
            'status': 'in_progress',
            'conclusion': None,
        },
    ]
    res = pfs.evaluate_checks(checks)
    self.assertEqual(res['total'], 1)
    self.assertTrue(res['all_completed'])
    self.assertEqual(res['failed'], [])
    self.assertEqual(res['sig'], 'all-green')

  def test_redacts_secrets_and_strips_exit_code_noise(self):
    log = '\n'.join([
        '2026-11-10T01:00:00Z \x1b[31mError compiling Cython file:\x1b[0m',
        '2026-11-10T01:00:01Z common.pxd:12:4: bad signature ghs_abcdefghijklmnopqrstuvwxyz1234',
        '2026-11-10T01:00:02Z ##[error]Process completed with exit code 1.',
    ])
    excerpt = pfs.extract_excerpt(log)
    self.assertIn('Error compiling Cython file:', excerpt)
    self.assertIn('[REDACTED]', excerpt)
    self.assertNotIn('ghs_', excerpt)
    self.assertNotIn('exit code 1', excerpt)

  def test_collapses_100_matrix_failures_into_root_causes(self):
    prelim = []
    for i in range(94):
      wf = f'PreCommit Python {i % 14}'
      prelim.append({
          'job_id': 1000 + i,
          'name': f'{wf} (py3{9 + i % 5})',
          'url': f'https://github.com/apache/beam/actions/runs/1/job/{1000 + i}',
          'workflow': wf,
          'step': 'Run Tox',
          'kind': 'cython',
      })
    for i in range(5):
      prelim.append({
          'job_id': 2000 + i,
          'name': f'Spotless {i}',
          'url': f'https://github.com/apache/beam/actions/runs/2/job/{2000 + i}',
          'workflow': 'PreCommit Spotless',
          'step': 'Spotless',
          'kind': 'spotless',
      })
    prelim.append({
        'job_id': 3000,
        'name': 'PreCommit Java Kafka IO',
        'url': 'https://github.com/apache/beam/actions/runs/3/job/3000',
        'workflow': 'PreCommit Java Kafka IO',
        'step': 'Run Gradle',
        'kind': 'flake',
    })

    sampled = pfs.select_stratified_sample(prelim, limit=24)
    self.assertEqual(len(sampled), 24)
    self.assertIn(3000, sampled)

    logs_by_kind = {
        'cython': 'Error compiling Cython file:\ncommon.pxd:10:2: mismatch',
        'spotless': "> Task :spotlessJavaCheck FAILED\nRun './gradlew spotlessApply'",
        'flake': '> Task :sdks:java:io:kafka:test FAILED\nTimeoutException',
    }
    enriched = []
    for it in prelim:
      is_sampled = it['job_id'] in sampled
      excerpt = pfs.extract_excerpt(logs_by_kind[it['kind']] if is_sampled else '')
      sig_key, title, hint = pfs.classify_excerpt(it['workflow'], it['step'], excerpt)
      enriched.append({
          **it,
          'sampled': is_sampled,
          'excerpt': excerpt,
          'sig_key': sig_key,
          'title': title,
          'hint': hint,
      })

    clusters = pfs.cluster_checks(enriched)
    self.assertEqual([len(c['checks']) for c in clusters], [94, 5, 1])

    comment = pfs.render_failure_comment(
        'apache/beam',
        'd9eccaa8d94cba',
        'abc123',
        {'total': 140, 'in_progress': 0, 'failed': prelim},
        clusters,
        None,
    )
    self.assertLessEqual(len(comment), pfs.MAX_COMMENT_CHARS)
    self.assertIn('94 checks across 14 workflows', comment)
    self.assertEqual(
        pfs.parse_sticky_state(comment),
        {'sha': 'd9eccaa8d94cba', 'status': 'failing', 'sig': 'abc123'})

  def test_parses_fenced_gemini_json_and_escapes_fences(self):
    wrapped = json.dumps({
        'response': '```json\n{"tldr": "Fix <pxd>", "clusters": []}\n```',
    })
    parsed = pfs.parse_gemini_output(wrapped)
    self.assertEqual(parsed, {'tldr': 'Fix <pxd>', 'clusters': []})
    fenced = pfs._safe_fence('line\n```\ninner\n````')
    self.assertTrue(fenced.startswith('`````text\n'))


if __name__ == '__main__':
  unittest.main()
