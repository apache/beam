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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializableComparator}. */
@RunWith(JUnit4.class)
public class SerializableComparatorTest {

  /**
   * Tests if the {@link SerializableComparator} returned by {@link
   * SerializableComparator#comparing(SerializableFunction)} using {@link
   * SerializableUtils#ensureSerializable(Serializable)}.
   */
  @Test
  public void testSerializable() {
    SerializableFunction<String, Integer> fn = Integer::parseInt;

    SerializableComparator<String> cmp = SerializableComparator.comparing(fn);
    SerializableUtils.ensureSerializable(cmp);
  }

  /**
   * Tests if {@link SerializableComparator#comparing(Function)} throws a {@link
   * java.lang.NullPointerException} if <code>null</code> is passed to it.
   */
  @Test(expected = NullPointerException.class)
  public void testIfNPEThrownForNullFunction() {
    SerializableComparator.comparing(null);
  }

  /** Tests the basic comparison function of the {@link SerializableComparator} returned. */
  @Test
  public void testBasicComparison() {
    SerializableFunction<String, Integer> fn = Integer::parseInt;
    SerializableComparator<String> cmp = SerializableComparator.comparing(fn);

    Assert.assertTrue(cmp.compare("1", "10") < 0);
    Assert.assertTrue(cmp.compare("9", "6") > 0);
  }
}
