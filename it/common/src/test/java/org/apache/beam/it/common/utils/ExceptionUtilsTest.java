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
package org.apache.beam.it.common.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.utils.ExceptionUtils.containsMessage;

import org.junit.Test;

public class ExceptionUtilsTest {

  @Test
  public void testContainsPositive() {
    assertThat(
            containsMessage(
                new IllegalStateException("RESOURCE_EXHAUSTED: Quota issues"),
                "RESOURCE_EXHAUSTED"))
        .isTrue();
  }

  @Test
  public void testContainsNegative() {
    assertThat(
            containsMessage(
                new IllegalStateException("RESOURCE_EXHAUSTED: Quota issues"),
                "NullPointerException"))
        .isFalse();
  }

  @Test
  public void testContainsPositiveNested() {
    assertThat(
            containsMessage(
                new IllegalStateException(
                    "There is a bad state in the client",
                    new IllegalArgumentException("RESOURCE_EXHAUSTED: Quota issues")),
                "RESOURCE_EXHAUSTED"))
        .isTrue();
  }

  @Test
  public void testContainsNegativeWithNested() {
    assertThat(
            containsMessage(
                new IllegalStateException(
                    "There is a bad state in the client",
                    new IllegalArgumentException("RESOURCE_EXHAUSTED: Quota issues")),
                "401 Unauthorized"))
        .isFalse();
  }
}
