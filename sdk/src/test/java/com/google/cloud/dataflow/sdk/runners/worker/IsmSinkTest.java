/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.WindowedValue.valueInEmptyWindows;

import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecord;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.IsmRecordCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.MetadataKeyCoder;
import com.google.cloud.dataflow.sdk.testing.CoderPropertiesTest.NonDeterministicCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink.SinkWriter;
import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link IsmSink}.
 *
 * <p>Note that {@link IsmReaderTest} covers reading/writing tests. This tests
 * error cases for the {@link IsmSink}.
 */
@RunWith(JUnit4.class)
public class IsmSinkTest {
  private static final IsmRecordCoder<byte[]> CODER =
      IsmRecordCoder.of(
          1, // number or shard key coders for value records
          1, // number of shard key coders for metadata records
          ImmutableList.<Coder<?>>of(MetadataKeyCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
          ByteArrayCoder.of());
  private static final byte[] EMPTY = new byte[0];
  private static final Coder<String> NON_DETERMINISTIC_CODER = new NonDeterministicCoder();


  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWriteOutOfOrderKeysWithSameShardKeyIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), CODER);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x01 }), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x00 }), EMPTY)));
  }

  @Test
  public void testWriteNonContiguousShardsIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), CODER);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(new byte[]{ 0x00 }, EMPTY), EMPTY)));
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(new byte[]{ 0x01 }, EMPTY), EMPTY)));

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("for shard which already exists");
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(new byte[]{ 0x00 }, EMPTY), EMPTY)));
  }

  @Test
  public void testWriteEqualKeysIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), CODER);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x01 }), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x01 }), EMPTY)));
  }

  @Test
  public void testWriteKeyWhichIsProperPrefixOfPreviousSecondaryKeyIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), CODER);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x00, 0x00 }), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(valueInEmptyWindows(
        IsmRecord.of(ImmutableList.of(EMPTY, new byte[]{ 0x00 }), EMPTY)));
  }

  @Test
  public void testUsingNonDeterministicShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmSink<>(tmpFolder.newFile().getPath(), IsmRecordCoder.of(
        1,
        0,
        ImmutableList.<Coder<?>>of(NON_DETERMINISTIC_CODER, ByteArrayCoder.of()),
        ByteArrayCoder.of()));
  }

  @Test
  public void testUsingNonDeterministicNonShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmSink<>(tmpFolder.newFile().getPath(), IsmRecordCoder.of(
        1,
        0,
        ImmutableList.<Coder<?>>of(ByteArrayCoder.of(), NON_DETERMINISTIC_CODER),
        ByteArrayCoder.of()));
  }
}
