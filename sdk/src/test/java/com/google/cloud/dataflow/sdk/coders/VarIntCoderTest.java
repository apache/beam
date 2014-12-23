/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for {@link VarIntCoder}.
 */
@RunWith(JUnit4.class)
public class VarIntCoderTest {

  private static final List<Integer> TEST_VALUES = Arrays.asList(
      -11, -3, -1, 0, 1, 5, 13, 29,
      Integer.MAX_VALUE,
      Integer.MIN_VALUE);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    Coder<Integer> coder = VarIntCoder.of();
    for (Integer value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(coder, value);
    }
  }
}

