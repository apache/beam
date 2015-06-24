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

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Sum.
 */
@RunWith(JUnit4.class)
public class SumTest {

  @Test
  public void testSumGetNames() {
    assertEquals("Sum.Globally", Sum.integersGlobally().getName());
    assertEquals("Sum.Globally", Sum.doublesGlobally().getName());
    assertEquals("Sum.Globally", Sum.longsGlobally().getName());
    assertEquals("Sum.PerKey", Sum.integersPerKey().getName());
    assertEquals("Sum.PerKey", Sum.doublesPerKey().getName());
    assertEquals("Sum.PerKey", Sum.longsPerKey().getName());
  }
}
