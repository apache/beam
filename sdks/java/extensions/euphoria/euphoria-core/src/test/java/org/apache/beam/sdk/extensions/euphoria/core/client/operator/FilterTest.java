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
import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.junit.Test;

/** Test operator Filter.  */
public class FilterTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> filtered = Filter.named("Filter1").of(dataset).by(s -> !s.equals("")).output();

    assertEquals(flow, filtered.getFlow());
    assertEquals(1, flow.size());

    Filter filter = (Filter) flow.operators().iterator().next();
    assertEquals(flow, filter.getFlow());
    assertEquals("Filter1", filter.getName());
    assertNotNull(filter.predicate);
    assertEquals(filtered, filter.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> filtered = Filter.of(dataset).by(s -> !s.equals("")).output();

    Filter filter = (Filter) flow.operators().iterator().next();
    assertEquals("Filter", filter.getName());
  }
}
