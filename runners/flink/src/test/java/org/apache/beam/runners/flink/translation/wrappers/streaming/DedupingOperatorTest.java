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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.DedupingOperator;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DedupingOperator}. */
@RunWith(JUnit4.class)
public class DedupingOperatorTest {

  @Test
  public void testDeduping() throws Exception {

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<ValueWithRecordId<String>>, WindowedValue<String>>
        harness = getDebupingHarness();

    harness.open();

    String key1 = "key1";
    String key2 = "key2";

    harness.processElement(
        new StreamRecord<>(
            WindowedValue.valueInGlobalWindow(
                new ValueWithRecordId<>(key1, key1.getBytes(StandardCharsets.UTF_8)))));

    harness.processElement(
        new StreamRecord<>(
            WindowedValue.valueInGlobalWindow(
                new ValueWithRecordId<>(key2, key2.getBytes(StandardCharsets.UTF_8)))));

    harness.processElement(
        new StreamRecord<>(
            WindowedValue.valueInGlobalWindow(
                new ValueWithRecordId<>(key1, key1.getBytes(StandardCharsets.UTF_8)))));

    assertThat(
        stripStreamRecordFromWindowedValue(harness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow(key1), WindowedValue.valueInGlobalWindow(key2)));

    OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);

    harness.close();

    harness = getDebupingHarness();
    harness.setup();
    harness.initializeState(snapshot);
    harness.open();

    String key3 = "key3";

    harness.processElement(
        new StreamRecord<>(
            WindowedValue.valueInGlobalWindow(
                new ValueWithRecordId<>(key2, key2.getBytes(StandardCharsets.UTF_8)))));

    harness.processElement(
        new StreamRecord<>(
            WindowedValue.valueInGlobalWindow(
                new ValueWithRecordId<>(key3, key3.getBytes(StandardCharsets.UTF_8)))));

    assertThat(
        stripStreamRecordFromWindowedValue(harness.getOutput()),
        contains(WindowedValue.valueInGlobalWindow(key3)));

    harness.close();
  }

  private KeyedOneInputStreamOperatorTestHarness<
          ByteBuffer, WindowedValue<ValueWithRecordId<String>>, WindowedValue<String>>
      getDebupingHarness() throws Exception {
    DedupingOperator<String> operator = new DedupingOperator<>(PipelineOptionsFactory.create());

    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        value -> ByteBuffer.wrap(value.getValue().getId()),
        TypeInformation.of(ByteBuffer.class));
  }
}
