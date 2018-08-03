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
package org.apache.beam.sdk.io.aws.redshift;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link Redshift}. */
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
    List<CommonPrefixTestCase> testCases =
        ImmutableList.of(
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
