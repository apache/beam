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
package org.apache.beam.sdk.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueState}. */
@RunWith(JUnit4.class)
public class ValueStateTest {

  @Test
  public void testReadReturnsDefaultValueWhenStateIsEmpty() {
    ValueState<Integer> state = new TestValueState<>();
    assertEquals(Integer.valueOf(5), state.read(5));
  }

  @Test
  public void testReadReturnsStoredValueWhenStateIsPresent() {
    TestValueState<Integer> state = new TestValueState<>();
    state.write(10);
    assertEquals(Integer.valueOf(10), state.read(5));
  }

  @Test
  public void testReadLaterReturnsSameState() {
    ValueState<Integer> state = new TestValueState<>();
    assertSame(state, state.readLater());
  }

  private static class TestValueState<T> implements ValueState<T> {

    private @Nullable T value;

    @Override
    public void write(T input) {
      value = input;
    }

    @Override
    public @Nullable T read() {
      return value;
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      value = null;
    }
  }
}
