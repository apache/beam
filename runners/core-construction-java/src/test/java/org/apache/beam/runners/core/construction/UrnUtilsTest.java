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

package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Tests for UrnUtils.
 */
public class UrnUtilsTest {

  private static final String GOOD_URN = "beam:coder:bytes:v1";
  private static final String MISSING_URN = "beam:fake:v1";
  private static final String BAD_URN = "Beam";

  @Test
  public void testGoodUrnSuccedes() {
    assertEquals(GOOD_URN, UrnUtils.validateCommonUrn(GOOD_URN));
  }

  @Test
  public void testMissingUrnFails() {
    try {
      UrnUtils.validateCommonUrn(MISSING_URN);
      fail("Should have rejected " + MISSING_URN);
    } catch (IllegalArgumentException exn) {
      // expected
    }
  }

  @Test
  public void testBadUrnFails() {
    try {
      UrnUtils.validateCommonUrn(BAD_URN);
      fail("Should have rejected " + BAD_URN);
    } catch (IllegalArgumentException exn) {
      // expected
    }
  }
}
