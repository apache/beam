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
package org.apache.beam.sdk.coders;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CoderFactories}.
 */
@RunWith(JUnit4.class)
public class CoderFactoriesTest {

  /**
   * Ensures that a few of our standard atomic coder classes
   * can each be built into a factory that works as expected.
   * It is presumed that testing a few, not all, suffices to
   * exercise CoderFactoryFromStaticMethods.
   */
  @Test
  public void testAtomicCoderClassFactories() {
    checkAtomicCoderFactory(StringUtf8Coder.class, StringUtf8Coder.of());
    checkAtomicCoderFactory(DoubleCoder.class, DoubleCoder.of());
    checkAtomicCoderFactory(ByteArrayCoder.class, ByteArrayCoder.of());
  }

  /**
   * Checks that {#link CoderFactories.fromStaticMethods} successfully
   * builds a working {@link CoderFactory} from {@link KvCoder KvCoder.class}.
   */
  @Test
  public void testKvCoderFactory() {
    CoderFactory kvCoderFactory = CoderFactories.fromStaticMethods(KvCoder.class);
    assertEquals(
        KvCoder.of(DoubleCoder.of(), DoubleCoder.of()),
        kvCoderFactory.create(Arrays.asList(DoubleCoder.of(), DoubleCoder.of())));
  }

  /**
   * Checks that {#link CoderFactories.fromStaticMethods} successfully
   * builds a working {@link CoderFactory} from {@link ListCoder ListCoder.class}.
   */
  @Test
  public void testListCoderFactory() {
    CoderFactory listCoderFactory = CoderFactories.fromStaticMethods(ListCoder.class);

    assertEquals(
        ListCoder.of(DoubleCoder.of()),
        listCoderFactory.create(Arrays.asList(DoubleCoder.of())));
  }

  /**
   * Checks that {#link CoderFactories.fromStaticMethods} successfully
   * builds a working {@link CoderFactory} from {@link IterableCoder IterableCoder.class}.
   */
  @Test
  public void testIterableCoderFactory() {
    CoderFactory iterableCoderFactory = CoderFactories.fromStaticMethods(IterableCoder.class);

    assertEquals(
        IterableCoder.of(DoubleCoder.of()),
        iterableCoderFactory.create(Arrays.asList(DoubleCoder.of())));
  }

  ///////////////////////////////////////////////////////////////////////

  /**
   * Checks that an atomic coder class can be converted into
   * a factory that then yields a coder equal to the example
   * provided.
   */
  private <T> void checkAtomicCoderFactory(
      Class<? extends Coder<T>> coderClazz,
      Coder<T> expectedCoder) {
    CoderFactory factory = CoderFactories.fromStaticMethods(coderClazz);
    @SuppressWarnings("unchecked")
    Coder<T> actualCoder = (Coder<T>) factory.create(Collections.<Coder<?>>emptyList());
    assertEquals(expectedCoder, actualCoder);
  }
}
