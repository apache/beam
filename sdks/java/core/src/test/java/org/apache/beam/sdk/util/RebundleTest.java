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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Rebundle}. Most operational properties (e.g. preservation of windows, handling
 * of late data etc.) are tested by {@link ReshuffleTest}.
 */
@RunWith(JUnit4.class)
public class RebundleTest {
  @Test
  @Category(RunnableOnService.class)
  public void testRebundle() {
    Pipeline pipeline = TestPipeline.create();

    String[] values = {"a", "b", "c", "d", "e", "f"};

    PAssert.that(pipeline.apply(Create.of(values)).apply(Rebundle.<String>create()))
        .containsInAnyOrder(values);

    pipeline.run();
  }
}
