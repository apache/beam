package org.apache.beam.sdk.io.aws.redshift;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link Redshift}.
 */
@RunWith(JUnit4.class)
public class RedshiftTest {

  static class CommonPrefixTestCase {

    final String left;
    final String right;
    final String result;

    CommonPrefixTestCase(String left, String right, String result) {
      this.left = left;
      this.right = right;
      this.result = result;
    }
  }

  @Test
  public void testLongestCommonPrefix() {
    List<CommonPrefixTestCase> testCases = ImmutableList.of(
        new CommonPrefixTestCase("abcdef", "abcxyz", "abc"),
        new CommonPrefixTestCase("abcdef", "xyzpdq", ""),
        new CommonPrefixTestCase("abcdef", "abcdef", "abcdef"),
        new CommonPrefixTestCase("abcdef", "", ""),
        new CommonPrefixTestCase("abcdef", null, ""));

    for (CommonPrefixTestCase testCase : testCases) {
      String gotResult = Redshift.longestCommonPrefix(testCase.left, testCase.right);
      assertEquals(testCase.result, gotResult);
      gotResult = Redshift.longestCommonPrefix(testCase.right, testCase.left);
      assertEquals(testCase.result, gotResult);
    }
  }
}
