package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the Tez TranslatorUtil class
 */
public class TranslatorUtilTest {

  @Test
  public void testDoFnSerialization() throws Exception {
    DoFn doFn = new testDoFn();
    String doFnString = TranslatorUtil.toString(doFn);
    DoFn newDoFn = (DoFn) TranslatorUtil.fromString(doFnString);
    Assert.assertEquals(newDoFn.getClass(), doFn.getClass());
  }

  private static class testDoFn extends DoFn {
    @ProcessElement
    public void processElement(ProcessContext c) {
      //Test DoFn
    }
  }
}
