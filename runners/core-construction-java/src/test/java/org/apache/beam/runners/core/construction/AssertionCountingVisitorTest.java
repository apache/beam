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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link AssertionCountingVisitor}.
 */
@RunWith(JUnit4.class)
public class AssertionCountingVisitorTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void getAssertCountCountsAsserts() {
    PCollection<Integer> create = pipeline.apply("FirstCreate", Create.of(1, 2, 3));

    PAssert.that(create).containsInAnyOrder(1, 2, 3);
    PAssert.thatSingleton(create.apply(Sum.integersGlobally())).isEqualTo(6);
    PAssert.thatMap(pipeline.apply("CreateMap", Create.of(KV.of(1, 2))))
        .isEqualTo(Collections.singletonMap(1, 2));

    AssertionCountingVisitor visitor = AssertionCountingVisitor.create();
    pipeline.traverseTopologically(visitor);
    assertThat(visitor.getPAssertCount(), equalTo(3));
  }

  @Test
  public void getAssertCountsPreVisitThrows(){
    AssertionCountingVisitor visitor = AssertionCountingVisitor.create();
    thrown.expect(IllegalStateException.class);
    visitor.getPAssertCount();
  }

  @Test
  public void visitMultipleTimesThrows() {
    AssertionCountingVisitor visitor = AssertionCountingVisitor.create();
    pipeline.traverseTopologically(visitor);
    thrown.expect(IllegalStateException.class);
    pipeline.traverseTopologically(visitor);
  }
}
