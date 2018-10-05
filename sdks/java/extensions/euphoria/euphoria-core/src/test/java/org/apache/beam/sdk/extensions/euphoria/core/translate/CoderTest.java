/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider.Factory;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareBinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Test;

/** Tests getting coder from functions and type aware functions. */
public class CoderTest {

  private final UnaryFunction<String, String> unaryFunction = a -> a;

  private final TranslationContext translationContext =
      new TranslationContext(
          mock(DAG.class),
          mock(Factory.class),
          TestPipeline.create(),
          mock(Settings.class),
          Duration.ZERO);

  @Test
  public void testGetCoder() {

    final Coder<String> coder = translationContext.getCoder(unaryFunction);
    assertEquals(StringUtf8Coder.class, coder.getClass());

    final Coder<String> coder2 =
        translationContext.getCoder(
            TypeAwareUnaryFunction.of(unaryFunction, TypeDescriptors.strings()));
    assertEquals(StringUtf8Coder.class, coder2.getClass());

    BinaryFunctor<Integer, Integer, Integer> binaryFunctor = (a, b, c) -> c.collect(a + b);
    final Coder<Integer> coder3 = translationContext.getCoder(binaryFunctor);
    assertEquals(KryoCoder.class, coder3.getClass());

    final Coder<Integer> coder4 =
        translationContext.getCoder(
            TypeAwareBinaryFunctor.of(binaryFunctor, TypeDescriptors.integers()));
    assertEquals(VarIntCoder.class, coder4.getClass());

    ReduceFunctor<Pair<Integer, Integer>, Pair<Integer, String>> reduceFunctor =
        (in, c) -> c.collect(Pair.of(1, ""));
    final Coder<Pair<Integer, String>> coder5 = translationContext.getCoder(reduceFunctor);
    assertEquals(KryoCoder.class, coder5.getClass());

    final UnaryFunction<String, Pair<String, String>> pairUnaryFunction = a -> Pair.of(a, a);
    final Coder<Pair<String, String>> coder6 = translationContext.getCoder(pairUnaryFunction);
    assertEquals(SerializableCoder.class, coder6.getClass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnregistredCoder() {
    ReduceFunctor<Pair<Integer, Integer>, Pair<NotSerializableClass, String>> reduceFunctor =
        (in, c) -> c.collect(Pair.of(new NotSerializableClass(), ""));

    translationContext.getCoder(
        TypeAwareReduceFunctor.of(
            reduceFunctor, TypeUtils.pairs(NotSerializableClass.class, String.class)));
  }

  private static class NotSerializableClass {

  }
}
