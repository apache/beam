package org.apache.beam.sdk.io.gcp.healthcare;


import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HttpBodyCoderTest {

  private static final HttpBodyCoder TEST_CODER = HttpBodyCoder.of();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (HttpBody value : FhirIOTestUtil.PRETTY_BUNDLES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }
}