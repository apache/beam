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

import java.util.ArrayList;
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
  public static final long PULL_FREQUENCY_SEC = 1L;
  public static final long START_POLL_TIMEOUT_SEC = 2L;
  public static final long START_OFFSET = 0L;

  static {
    OPTIONS.setBlockOnRun(false);
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(OPTIONS);

  @Test
  public void testReadBuildsCorrectly() {
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SerializableFunction<String, Long> offsetFn = Long::valueOf;
    SerializableFunction<String, Instant> timestampFn = Instant::parse;

    SparkReceiverIO.Read<String> read =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(offsetFn)
            .withTimestampFn(timestampFn)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
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
  public void testReadObjectCreationFailsIfTimestampFnIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> SparkReceiverIO.<String>read().withTimestampFn(null));
  }

  @Test
  public void testReadObjectCreationFailsIfPullFrequencySecIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SparkReceiverIO.<String>read().withPullFrequencySec(null));
  }

  @Test
  public void testReadObjectCreationFailsIfStartPollTimeoutSecIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SparkReceiverIO.<String>read().withStartPollTimeoutSec(null));
  }

  @Test
  public void testReadObjectCreationFailsIfStartOffsetIsNull() {
    assertThrows(
        IllegalArgumentException.class, () -> SparkReceiverIO.<String>read().withStartOffset(null));
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
    CustomReceiverWithOffset.shouldFailInTheMiddle = false;
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withTimestampFn(Instant::parse)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < CustomReceiverWithOffset.RECORDS_COUNT; i++) {
      expected.add(String.valueOf(i));
    }
    PCollection<String> actual = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }

  @Test
  public void testReadFromCustomReceiverWithOffsetFailsAndReread() {
    CustomReceiverWithOffset.shouldFailInTheMiddle = true;
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withTimestampFn(Instant::parse)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < CustomReceiverWithOffset.RECORDS_COUNT; i++) {
      expected.add(String.valueOf(i));
    }
    PCollection<String> actual = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }

  @Test
  public void testReadFromReceiverArrayBufferData() {
    ReceiverBuilder<String, ArrayBufferDataReceiver> receiverBuilder =
        new ReceiverBuilder<>(ArrayBufferDataReceiver.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withTimestampFn(Instant::parse)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < ArrayBufferDataReceiver.RECORDS_COUNT; i++) {
      expected.add(String.valueOf(i));
    }
    PCollection<String> actual = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }

  @Test
  public void testReadFromReceiverByteBufferData() {
    ReceiverBuilder<String, ByteBufferDataReceiver> receiverBuilder =
        new ReceiverBuilder<>(ByteBufferDataReceiver.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withTimestampFn(Instant::parse)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < ByteBufferDataReceiver.RECORDS_COUNT; i++) {
      expected.add(String.valueOf(i));
    }
    PCollection<String> actual = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }

  @Test
  public void testReadFromReceiverIteratorData() {
    ReceiverBuilder<String, IteratorDataReceiver> receiverBuilder =
        new ReceiverBuilder<>(IteratorDataReceiver.class).withConstructorArgs();
    SparkReceiverIO.Read<String> reader =
        SparkReceiverIO.<String>read()
            .withGetOffsetFn(Long::valueOf)
            .withTimestampFn(Instant::parse)
            .withPullFrequencySec(PULL_FREQUENCY_SEC)
            .withStartPollTimeoutSec(START_POLL_TIMEOUT_SEC)
            .withStartOffset(START_OFFSET)
            .withSparkReceiverBuilder(receiverBuilder);

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < IteratorDataReceiver.RECORDS_COUNT; i++) {
      expected.add(String.valueOf(i));
    }
    PCollection<String> actual = pipeline.apply(reader).setCoder(StringUtf8Coder.of());

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish(Duration.standardSeconds(15));
  }
}
