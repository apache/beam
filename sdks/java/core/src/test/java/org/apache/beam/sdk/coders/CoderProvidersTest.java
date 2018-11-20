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
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CoderProviders}. */
@RunWith(JUnit4.class)
public class CoderProvidersTest {
  @Test
  public void testCoderProvidersFromStaticMethodsForParameterlessTypes() throws Exception {
    CoderProvider factory = CoderProviders.fromStaticMethods(String.class, StringUtf8Coder.class);
    assertEquals(
        StringUtf8Coder.of(), factory.coderFor(TypeDescriptors.strings(), Collections.emptyList()));

    factory = CoderProviders.fromStaticMethods(Double.class, DoubleCoder.class);
    assertEquals(
        DoubleCoder.of(), factory.coderFor(TypeDescriptors.doubles(), Collections.emptyList()));

    factory = CoderProviders.fromStaticMethods(byte[].class, ByteArrayCoder.class);
    assertEquals(
        ByteArrayCoder.of(),
        factory.coderFor(TypeDescriptor.of(byte[].class), Collections.emptyList()));
  }

  /**
   * Checks that {#link CoderProviders.fromStaticMethods} successfully builds a working {@link
   * CoderProvider} from {@link KvCoder KvCoder.class}.
   */
  @Test
  public void testKvCoderProvider() throws Exception {
    TypeDescriptor<KV<Double, Double>> type =
        TypeDescriptors.kvs(TypeDescriptors.doubles(), TypeDescriptors.doubles());
    CoderProvider kvCoderProvider = CoderProviders.fromStaticMethods(KV.class, KvCoder.class);
    assertEquals(
        KvCoder.of(DoubleCoder.of(), DoubleCoder.of()),
        kvCoderProvider.coderFor(type, Arrays.asList(DoubleCoder.of(), DoubleCoder.of())));
  }

  /**
   * Checks that {#link CoderProviders.fromStaticMethods} successfully builds a working {@link
   * CoderProvider} from {@link ListCoder ListCoder.class}.
   */
  @Test
  public void testListCoderProvider() throws Exception {
    TypeDescriptor<List<Double>> type = TypeDescriptors.lists(TypeDescriptors.doubles());
    CoderProvider listCoderProvider = CoderProviders.fromStaticMethods(List.class, ListCoder.class);

    assertEquals(
        ListCoder.of(DoubleCoder.of()),
        listCoderProvider.coderFor(type, Arrays.asList(DoubleCoder.of())));
  }

  /**
   * Checks that {#link CoderProviders.fromStaticMethods} successfully builds a working {@link
   * CoderProvider} from {@link IterableCoder IterableCoder.class}.
   */
  @Test
  public void testIterableCoderProvider() throws Exception {
    TypeDescriptor<Iterable<Double>> type = TypeDescriptors.iterables(TypeDescriptors.doubles());
    CoderProvider iterableCoderProvider =
        CoderProviders.fromStaticMethods(Iterable.class, IterableCoder.class);

    assertEquals(
        IterableCoder.of(DoubleCoder.of()),
        iterableCoderProvider.coderFor(type, Arrays.asList(DoubleCoder.of())));
  }
}
