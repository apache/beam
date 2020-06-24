package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryResourceNaming.JobType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryResourceNamingTest {

  @Test
  public void testJobTypesInNames() {
    assertEquals(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.EXPORT),
        "beam_job_EXPORT_beamappjobtest_abcd");

    assertEquals(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.LOAD),
        "beam_job_LOAD_beamappjobtest_abcd");

    assertEquals(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.QUERY),
        "beam_job_QUERY_beamappjobtest_abcd");

    assertEquals(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.COPY),
        "beam_job_COPY_beamappjobtest_abcd");
  }

  @Test
  public void testJobRandomInNames() {
    assertEquals(
        BigQueryResourceNaming.createJobIdPrefix(
            "beamapp-job-test", "abcd", JobType.EXPORT, "RANDOME"),
        "beam_job_EXPORT_beamappjobtest_abcd_RADOME");
  }

}
