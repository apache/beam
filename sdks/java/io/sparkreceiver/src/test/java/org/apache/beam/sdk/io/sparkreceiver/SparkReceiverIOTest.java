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
package org.apache.beam.sdk.io.sparkreceiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SparkReceiverIO}. */
@RunWith(JUnit4.class)
public class SparkReceiverIOTest {

  public static final TestPipelineOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

  static {
    OPTIONS.setBlockOnRun(false);
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);

  @Test
  public void testReadBuildsCorrectly() {
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SerializableFunction<String, Long> offsetFn = Long::valueOf;
    SerializableFunction<String, Instant> watermarkFn = Instant::parse;

    SparkReceiverIO.Read<String> read =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(offsetFn)
            .withWatermarkFn(watermarkFn)
            .withSparkReceiverBuilder(receiverBuilder);

    assertEquals(offsetFn, read.getGetOffsetFn());
    assertEquals(receiverBuilder, read.getSparkReceiverBuilder());
  }

  @Test
  public void testReadObjectCreationFailsIfReceiverBuilderIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SparkReceiverIO.<String>read().withSparkReceiverBuilder(null));
  }

  @Test
  public void testReadObjectCreationFailsIfGetOffsetFnIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> SparkReceiverIO.<String>read().withGetOffsetFn(null));
  }

  @Test
  public void testReadObjectCreationFailsIfWatermarkFnIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> SparkReceiverIO.<String>read().withWatermarkFn(null));
  }

  @Test
  public void testReadValidationFailsMissingReceiverBuilder() {
    SparkReceiverIO.Read<String> read = SparkReceiverIO.read();
    assertThrows(IllegalStateException.class, read::validateTransform);
  }

  @Test
  public void testReadValidationFailsMissingSparkConsumer() {
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SparkReceiverIO.Read<String> read =
        SparkReceiverIO.<String>read().withSparkReceiverBuilder(receiverBuilder);
    assertThrows(IllegalStateException.class, read::validateTransform);
  }

  @Test
  public void testReadFromCustomReceiverWithOffset() {
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withWatermarkFn(Instant::parse)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> storedRecords = CustomReceiverWithOffset.getStoredRecords();

    PCollection<String> output = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(output).containsInAnyOrder(storedRecords);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }
}
