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
package org.apache.beam.runners.dataflow.worker.graph;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.HappensBeforeEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Edges}. */
@RunWith(JUnit4.class)
public class EdgesTest {
  @Test
  public void testDefaultEdge() {
    DefaultEdge edge = DefaultEdge.create();
    assertNotEquals(edge, DefaultEdge.create());
    assertNotEquals(edge, edge.clone());
  }

  @Test
  public void testInstructionOutputNode() {
    MultiOutputInfo param = new MultiOutputInfo();
    MultiOutputInfoEdge edge = MultiOutputInfoEdge.create(param);
    assertSame(param, edge.getMultiOutputInfo());
    assertNotEquals(edge, MultiOutputInfoEdge.create(param));
    assertNotEquals(edge, edge.clone());
    assertSame(param, MultiOutputInfoEdge.create(param).clone().getMultiOutputInfo());
  }

  @Test
  public void testHappensBeforeEdge() {
    HappensBeforeEdge edge = HappensBeforeEdge.create();
    assertNotEquals(edge, HappensBeforeEdge.create());
    assertNotEquals(edge, edge.clone());
  }
}
