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
package org.apache.beam.sdk.transforms.errorhandling;

import java.util.Objects;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord.Record;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
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
  public void testNoUsageErrorHandlerUsage() throws Exception {
    try (BadRecordErrorHandler<PCollection<BadRecord>> eh =
        pipeline.registerBadRecordErrorHandler(new DummySinkTransform<>())) {}

    pipeline.run();
  }

  @Test
  public void testUnclosedErrorHandlerUsage() {

    pipeline.registerBadRecordErrorHandler(new DummySinkTransform<>());

    // Expected to be thrown because the error handler isn't closed
    thrown.expect(IllegalStateException.class);

    pipeline.run();
  }

  @Test
  public void testBRHEnabledPTransform() {
    PCollection<Integer> record = pipeline.apply(Create.of(1, 2, 3, 4));
    record.apply(new BRHEnabledPTransform());

    // unhandled runtime exception thrown by the BRHEnabledPTransform
    thrown.expect(RuntimeException.class);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testErrorHandlerWithBRHTransform() throws Exception {
    PCollection<Integer> record = pipeline.apply(Create.of(1, 2, 3, 4));
    DummySinkTransform<BadRecord> transform = new DummySinkTransform<>();
    ErrorHandler<BadRecord, PCollection<BadRecord>> eh =
        pipeline.registerBadRecordErrorHandler(transform);
    record.apply(new BRHEnabledPTransform().withBadRecordHandler(eh));
    eh.close();
    PCollection<BadRecord> badRecords = eh.getOutput();

    // We use a more complex satisfies statement to ensure we don't need to preserve stacktraces
    // in test cases
    PAssert.that(badRecords)
        .satisfies(
            (records) -> {
              int count = 0;
              for (BadRecord badRecord : records) {
                count++;

                Record r = null;

                if (Objects.equals(badRecord.getRecord().getHumanReadableJsonRecord(), "1")) {
                  r =
                      Record.builder()
                          .setHumanReadableJsonRecord("1")
                          .setEncodedRecord(new byte[] {0, 0, 0, 1})
                          .setCoder("BigEndianIntegerCoder")
                          .build();
                } else {
                  r =
                      Record.builder()
                          .setHumanReadableJsonRecord("3")
                          .setEncodedRecord(new byte[] {0, 0, 0, 3})
                          .setCoder("BigEndianIntegerCoder")
                          .build();
                }

                BadRecord.Builder expectedBuilder = BadRecord.builder().setRecord(r);

                BadRecord.Failure.Builder failure =
                    BadRecord.Failure.builder()
                        .setException("java.lang.RuntimeException: Integer was odd")
                        .setDescription("Integer was odd");

                failure.setExceptionStacktrace(badRecord.getFailure().getExceptionStacktrace());
                expectedBuilder.setFailure(failure.build());
                Assert.assertEquals("Expect failure to match", expectedBuilder.build(), badRecord);
              }
              Assert.assertEquals("Expect 2 errors", 2, count);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  public static class DummySinkTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input;
    }
  }
}
