package org.apache.beam.sdk.io.gcp.bigquery;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link BigQueryWriteResultCoder}. */
@RunWith(JUnit4.class)
public class BigQueryWriteResultCoderTest {

  private static final Coder<BigQueryWriteResult> TEST_CODER = BigQueryWriteResultCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    BigQueryWriteResult value =
        new BigQueryWriteResult(BigQueryHelpers.Status.SUCCEEDED, "dummy_table");

    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
  }
}
