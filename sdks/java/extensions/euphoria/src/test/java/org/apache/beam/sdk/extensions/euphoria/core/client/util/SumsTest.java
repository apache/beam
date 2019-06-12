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
package org.apache.beam.sdk.extensions.euphoria.core.client.util;

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;
import org.junit.Test;

/** Test suite for @{link Sums}. */
public class SumsTest {

  @Test
  public void testSumOfInts() {
    assertEquals(6, (int) Sums.ofInts().apply(Stream.of(1, 2, 3)));
  }

  @Test
  public void testSumOfLongs() {
    assertEquals(6L, (long) Sums.ofLongs().apply(Stream.of(1L, 2L, 3L)));
  }

  @Test
  public void testSumOfFloats() {
    assertEquals(6f, (float) Sums.ofFloats().apply(Stream.of(1f, 2f, 3f)), 0.001);
  }

  @Test
  public void testSumOfDoubles() {
    assertEquals(6.0, (double) Sums.ofDoubles().apply(Stream.of(1.0, 2.0, 3.0)), 0.001);
  }
}
