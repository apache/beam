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
package org.apache.beam.sdk.errorhandling;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ErrorHandlerTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void testGoodErrorHandlerUsage() throws Exception {
    try (ErrorHandler<String, PCollection<String>> eh =
        pipeline.registerErrorHandler(new DummySinkTransform<>())) {}

    pipeline.run();
  }

  @Test
  public void testBadErrorHandlerUsage() {

    pipeline.registerErrorHandler(new DummySinkTransform<PCollection<String>>());

    thrown.expect(IllegalStateException.class);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBRHEnabledPTransform() {
    PCollection<Integer> record = pipeline.apply(Create.of(1, 2, 3, 4));
    record.apply(new BRHEnabledPTransform());

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testErrorHandlerWithBRHTransform() throws Exception {
    PCollection<Integer> record = pipeline.apply(Create.of(1, 2, 3, 4));
    try (ErrorHandler<BadRecord, PCollection<BadRecord>> eh =
        pipeline.registerErrorHandler(new DummySinkTransform<>())) {
      record.apply(new BRHEnabledPTransform().withBadRecordHandler(eh));
    }

    pipeline.run();
  }

  public static class DummySinkTransform<T extends PCollection<?>> extends PTransform<T, T> {

    @Override
    public T expand(T input) {
      return input;
    }
  }
}
