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
package org.apache.beam.sdk.extensions.kryo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

/** A collections of {@link KryoCoderProvider} unit tests. */
public class KryoCoderProviderTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuilding() {
    KryoCoderProvider provider = KryoCoderProvider.of(pipeline.getOptions());
    assertNotNull(provider);
  }

  @Test
  public void testBuildingAndRegister() {
    KryoCoderProvider.of(pipeline.getOptions()).registerTo(pipeline);
  }

  @Test
  public void testItProvidesCodersToRegisteredClasses() throws CannotProvideCoderException {
    final KryoCoderProvider provider =
        KryoCoderProvider.of(
            pipeline.getOptions(),
            kryo -> {
              kryo.register(FirstTestClass.class);
              kryo.register(SecondTestClass.class);
              kryo.register(ThirdTestClass.class);
            });
    assertProviderReturnsKryoCoderForClass(provider, FirstTestClass.class);
    assertProviderReturnsKryoCoderForClass(provider, SecondTestClass.class);
    assertProviderReturnsKryoCoderForClass(provider, ThirdTestClass.class);
  }

  @Test(expected = CannotProvideCoderException.class)
  public void testDoNotProvideCoderForUnregisteredClasses() throws CannotProvideCoderException {
    final KryoCoderProvider provider =
        KryoCoderProvider.of(
            pipeline.getOptions(),
            kryo -> {
              kryo.register(FirstTestClass.class);
              kryo.register(SecondTestClass.class);
              kryo.register(ThirdTestClass.class);
            });
    provider.coderFor(TypeDescriptor.of(NeverRegisteredClass.class), Collections.emptyList());
  }

  @Test
  public void testProviderRegisteredToPipeline() throws CannotProvideCoderException {
    KryoCoderProvider.of(pipeline.getOptions(), kryo -> kryo.register(FirstTestClass.class))
        .registerTo(pipeline);
    final Coder<FirstTestClass> coderToAssert =
        pipeline.getCoderRegistry().getCoder(FirstTestClass.class);
    assertNotNull(coderToAssert);
    assertTrue(coderToAssert instanceof KryoCoder);
    final KryoCoder<FirstTestClass> casted = (KryoCoder<FirstTestClass>) coderToAssert;
    assertEquals(1, casted.getRegistrars().size());
  }

  @Test(expected = CannotProvideCoderException.class)
  public void testPrimitiveClass() throws CannotProvideCoderException {
    final KryoCoderProvider provider = KryoCoderProvider.of(pipeline.getOptions());
    provider.coderFor(TypeDescriptors.strings(), Collections.emptyList());
  }

  private static <T> void assertProviderReturnsKryoCoderForClass(
      KryoCoderProvider provider, Class<T> type) throws CannotProvideCoderException {
    assertTrue(provider.getCoder().getRegistrars().size() > 0);
    final Coder<T> coderToAssert =
        provider.coderFor(TypeDescriptor.of(type), Collections.emptyList());
    assertNotNull(coderToAssert);
    assertTrue(coderToAssert instanceof KryoCoder);
    final KryoCoder<T> casted = (KryoCoder<T>) coderToAssert;
    assertEquals(provider.getCoder().getInstanceId(), casted.getInstanceId());
  }

  private static class FirstTestClass {}

  private static class SecondTestClass {}

  private static class ThirdTestClass {}

  private static class NeverRegisteredClass {}
}
