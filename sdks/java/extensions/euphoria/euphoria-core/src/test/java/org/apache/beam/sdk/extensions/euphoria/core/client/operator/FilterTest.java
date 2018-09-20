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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test operator Filter. */
public class FilterTest {

  @Test
  public void testBuild() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> filtered =
        Filter.named("Filter1").of(dataset).by(s -> !s.equals("")).output();
    assertTrue(filtered.getProducer().isPresent());
    final Filter filter = (Filter) filtered.getProducer().get();
    assertTrue(filter.getName().isPresent());
    assertEquals("Filter1", filter.getName().get());
    assertNotNull(filter.getPredicate());
  }

  @Test
  public void testBuild_implicitName() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> filtered = Filter.of(dataset).by(s -> !s.equals("")).output();
    assertTrue(filtered.getProducer().isPresent());
    final Filter filter = (Filter) filtered.getProducer().get();
    assertFalse(filter.getName().isPresent());
  }
}
