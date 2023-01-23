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
package org.apache.beam.sdk.extensions.avro.coders;

import static org.junit.Assert.assertEquals;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviderRegistrar;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for CoderRegistry and AvroCoder. */
@RunWith(JUnit4.class)
public class CoderRegistryTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(CoderRegistry.class);

  @Test
  public void testCoderPrecedence() throws Exception {
    CoderRegistry registry = CoderRegistry.createDefault();

    // DefaultCoder precedes CoderProviderRegistrar
    assertEquals(AvroCoder.of(MyValueA.class), registry.getCoder(MyValueA.class));

    // CoderProviderRegistrar precedes SerializableCoder
    assertEquals(MyValueBCoder.INSTANCE, registry.getCoder(MyValueB.class));

    // fallbacks to SerializableCoder at last
    assertEquals(SerializableCoder.of(MyValueC.class), registry.getCoder(MyValueC.class));
  }

  @DefaultCoder(AvroCoder.class)
  private static class MyValueA implements Serializable {}

  private static class MyValueB implements Serializable {}

  private static class MyValueC implements Serializable {}

  private static class MyValueACoder extends CustomCoder<MyValueA> {
    private static final MyValueACoder INSTANCE = new MyValueACoder();

    @Override
    public void encode(MyValueA value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public MyValueA decode(InputStream inStream) throws CoderException, IOException {
      return null;
    }
  }

  /** A {@link CoderProviderRegistrar} to demonstrate default {@link Coder} registration. */
  @AutoService(CoderProviderRegistrar.class)
  public static class MyValueACoderProviderRegistrar implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(
          CoderProviders.forCoder(TypeDescriptor.of(MyValueA.class), MyValueACoder.INSTANCE));
    }
  }

  private static class MyValueBCoder extends CustomCoder<MyValueB> {
    private static final MyValueBCoder INSTANCE = new MyValueBCoder();

    @Override
    public void encode(MyValueB value, OutputStream outStream) throws CoderException, IOException {}

    @Override
    public MyValueB decode(InputStream inStream) throws CoderException, IOException {
      return null;
    }
  }

  /** A {@link CoderProviderRegistrar} to demonstrate default {@link Coder} registration. */
  @AutoService(CoderProviderRegistrar.class)
  public static class MyValueBCoderProviderRegistrar implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(
          CoderProviders.forCoder(TypeDescriptor.of(MyValueB.class), MyValueBCoder.INSTANCE));
    }
  }
}
