/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;

/**
 * Tests for PCollectionLists.
 */
@RunWith(JUnit4.class)
public class PCollectionListTest {
  @Test
  public void testEmptyListFailure() {
    try {
      PCollectionList.of(Collections.<PCollection<String>>emptyList());
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      assertThat(
          exn.toString(),
          containsString(
              "must either have a non-empty list of PCollections, " +
              "or must first call empty(Pipeline)"));
    }
  }
}
