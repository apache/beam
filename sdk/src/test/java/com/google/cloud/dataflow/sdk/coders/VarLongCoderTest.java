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
 * Test case for {@link VarLongCoder}.
 */
@RunWith(JUnit4.class)
public class VarLongCoderTest {

  private static final List<Long> TEST_VALUES = Arrays.asList(
      -11L, -3L, -1L, 0L, 1L, 5L, 13L, 29L,
      Integer.MAX_VALUE + 131L,
      Integer.MIN_VALUE - 29L,
      Long.MAX_VALUE,
      Long.MIN_VALUE);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    Coder<Long> coder = VarLongCoder.of();
    for (Long value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(coder, value);
    }
  }
}
