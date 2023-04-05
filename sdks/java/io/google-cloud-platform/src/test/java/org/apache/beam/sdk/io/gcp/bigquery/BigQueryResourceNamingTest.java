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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryResourceNamingTest {

  public static final String BQ_JOB_PATTERN_REGEXP =
      "beam_bq_job_[A-Z]+_[a-z0-9-]+_[a-z0-9-]+(_[A-Za-z0-9-]+)?";

  @Test
  public void testJobTypesInNames() {
    assertEquals(
        "beam_bq_job_EXPORT_beamappjobtest_abcd",
        BigQueryResourceNaming.createJobIdPrefix("beamapp-job-test", "abcd", JobType.EXPORT));

    assertEquals(
        "beam_bq_job_LOAD_beamappjobtest_abcd",
        BigQueryResourceNaming.createJobIdPrefix("beamapp-job-test", "abcd", JobType.LOAD));

    assertEquals(
        "beam_bq_job_QUERY_beamappjobtest_abcd",
        BigQueryResourceNaming.createJobIdPrefix("beamapp-job-test", "abcd", JobType.QUERY));

    assertEquals(
        "beam_bq_job_COPY_beamappjobtest_abcd",
        BigQueryResourceNaming.createJobIdPrefix("beamapp-job-test", "abcd", JobType.COPY));
  }

  @Test
  public void testJobRandomInNames() {
    assertEquals(
        "beam_bq_job_EXPORT_beamappjobtest_abcd_RANDOME",
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.EXPORT, "RANDOME"));
  }

  @Test
  public void testMatchesBigQueryJobTemplate() {
    assertThat(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.EXPORT, "RANDOME"),
        matchesPattern(BQ_JOB_PATTERN_REGEXP));

    assertThat(
        BigQueryResourceNaming.createJobIdPrefix("beamapp-job-test", "abcd", JobType.COPY),
        matchesPattern(BQ_JOB_PATTERN_REGEXP));
  }
}
