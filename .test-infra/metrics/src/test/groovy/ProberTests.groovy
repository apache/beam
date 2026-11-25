/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.testinfra.metrics

import org.junit.Test
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
 * Prober tests which performs health checks on deployed infrastructure for
 * community metrics.
 */
class ProberTests {
  // TODO: Make this configurable
  def grafanaEndpoint = 'https://metrics.beam.apache.org'

  @Test
  void PingGrafanaHttpApi() {
    def allDashboardsJson = "${grafanaEndpoint}/api/search?type=dash-db".toURL().text
    def allDashboards = new JsonSlurper().parseText(allDashboardsJson)
    def dashboardNames = allDashboards.title
    // Validate at least one expected dashboard exists
    assert dashboardNames.contains('Post-commit Test Reliability') : 'Expected dashboard does not exist'
    assert dashboardNames.size() > 0 : "No dashboards found. Check Grafana dashboard initialization script."
  }

  /**
   * Queries Community Metrics data through the Grafana API and verifies that
   * GitHub Issues and Pull Requests metrics are present and have been
   * synchronized within the last 8 hours.
   */
  @Test
  void CheckGrafanaDataFreshness() {
    def query = '''
    WITH sources AS (
      SELECT
        'GitHub Issues' AS source,
        timestamp AS last_sync
      FROM gh_sync_metadata
      WHERE name = 'gh_issue_sync'

      UNION ALL

      SELECT
        'GitHub Pull Requests' AS source,
        timestamp AS last_sync
      FROM gh_sync_metadata
      WHERE name = 'gh_pr_sync'
    )
    SELECT
      COUNT(*) AS source_count,
      COUNT(last_sync) AS synced_source_count,
      MIN(EXTRACT(EPOCH FROM age(current_timestamp, last_sync)) / 60)
        AS min_staleness_minutes,
      MAX(EXTRACT(EPOCH FROM age(current_timestamp, last_sync)) / 60)
        AS max_staleness_minutes
    FROM sources
  '''

    def requestBody = [
      queries: [[
                  refId: 'A',
                  datasource: [
                    uid: 'beampsql',
                    type: 'grafana-postgresql-datasource'
                  ],
                  editorMode: 'code',
                  format: 'table',
                  rawQuery: true,
                  rawSql: query
                ]],
      from: 'now-5m',
      to: 'now'
    ]

    def connection = new URL("${grafanaEndpoint}/api/ds/query").openConnection()
    connection.requestMethod = 'POST'
    connection.doOutput = true
    connection.setRequestProperty('Content-Type', 'application/json')

    connection.outputStream.withWriter('UTF-8') {
      it << JsonOutput.toJson(requestBody)
    }

    assert connection.responseCode == 200 :
      "Grafana datasource query failed with HTTP ${connection.responseCode}"

    def response = new JsonSlurper().parse(connection.inputStream)
    def frame = response.results.A.frames[0]

    def fieldNames = frame.schema.fields.collect { it.name }

    def value = { String fieldName ->
      def index = fieldNames.indexOf(fieldName)
      assert index >= 0 : "Missing field '${fieldName}' in Grafana response"
      frame.data.values[index][0]
    }

    def sourceCount = value('source_count')
    def syncedSourceCount = value('synced_source_count')
    def minStalenessMinutes = value('min_staleness_minutes')
    def maxStalenessMinutes = value('max_staleness_minutes')

    assert sourceCount == 2 :
      "Expected 2 metrics sources, got ${sourceCount}"

    assert syncedSourceCount == 2 :
      "One or more metrics sources have no synchronization timestamp"

    assert minStalenessMinutes != null
    assert maxStalenessMinutes != null

    // gh_sync_metadata.timestamp stores the updatedAt timestamp of the most
    // recently updated GitHub Issue (gh_issue_sync) and PR (gh_pr_sync), both
    // updated in the same sync loop. Verify that at least one source has seen
    // synced activity within 8 hours (480m) and both within 48 hours (2880m) to
    // avoid false positives during quiet overnight/weekend issue windows.
    assert minStalenessMinutes <= 480 :
      "Community metrics sync appears stalled: neither GitHub Issues (gh_issue_sync) nor GitHub Pull Requests (gh_pr_sync) " +
        "has recorded any updated activity within the last 8 hours (480 minutes); the most recently updated source was last synced " +
        "${minStalenessMinutes} minutes ago.\n" +
        "See: ${grafanaEndpoint}/d/data-freshness"

    assert maxStalenessMinutes <= 2880 :
      "At least one community metrics source (GitHub Issues gh_issue_sync or GitHub Pull Requests gh_pr_sync) " +
        "has not recorded any updated activity within the last 48 hours (2880 minutes); the least recently updated source was last synced " +
        "${maxStalenessMinutes} minutes ago.\n" +
        "See: ${grafanaEndpoint}/d/data-freshness"
  }
}
