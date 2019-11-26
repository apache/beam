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
package org.apache.beam.runners.dataflow.worker;

import java.util.Arrays;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink.SinkWriter;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.CoderPropertiesTest.NonDeterministicCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link IsmSink}.
 *
 * <p>Note that {@link IsmReaderTest} covers reading/writing tests. This tests error cases for the
 * {@link IsmSink}.
 */
@RunWith(JUnit4.class)
public class IsmSinkTest {
  private static final long BLOOM_FILTER_SIZE_LIMIT = 10_000;
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
  public void testWriteEmptyKeyWithValueLargerThanBlockSize() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
            IsmRecordCoder.of(
                1, // We hash using only the window
                0, // There are no metadata records
                // We specifically use a coder that encodes to 0 bytes.
                ImmutableList.<Coder<?>>of(VoidCoder.of()),
                ByteArrayCoder.of()),
            BLOOM_FILTER_SIZE_LIMIT);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(
        new ValueInEmptyWindows<>(
            IsmRecord.of(
                Arrays.asList(new Object[] {null}), new byte[IsmSink.BLOCK_SIZE_BYTES * 2])));
    sinkWriter.close();
  }

  @Test
  public void testWriteOutOfOrderKeysWithSameShardKeyIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
            CODER,
            BLOOM_FILTER_SIZE_LIMIT);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x01}), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x00}), EMPTY)));
  }

  @Test
  public void testWriteNonContiguousShardsIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
            CODER,
            BLOOM_FILTER_SIZE_LIMIT);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(new byte[] {0x00}, EMPTY), EMPTY)));
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(new byte[] {0x01}, EMPTY), EMPTY)));

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("for shard which already exists");
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(new byte[] {0x00}, EMPTY), EMPTY)));
  }

  @Test
  public void testWriteEqualKeysIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
            CODER,
            BLOOM_FILTER_SIZE_LIMIT);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x01}), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x01}), EMPTY)));
  }

  @Test
  public void testWriteKeyWhichIsProperPrefixOfPreviousSecondaryKeyIsError() throws Throwable {
    IsmSink<byte[]> sink =
        new IsmSink<>(
            FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
            CODER,
            BLOOM_FILTER_SIZE_LIMIT);
    SinkWriter<WindowedValue<IsmRecord<byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(
        new ValueInEmptyWindows<>(
            IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x00, 0x00}), EMPTY)));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(
        new ValueInEmptyWindows<>(IsmRecord.of(ImmutableList.of(EMPTY, new byte[] {0x00}), EMPTY)));
  }

  @Test
  public void testUsingNonDeterministicShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmSink<>(
        FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.<Coder<?>>of(NON_DETERMINISTIC_CODER, ByteArrayCoder.of()),
            ByteArrayCoder.of()),
        BLOOM_FILTER_SIZE_LIMIT);
  }

  @Test
  public void testUsingNonDeterministicNonShardKeyCoder() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("is expected to be deterministic");
    new IsmSink<>(
        FileSystems.matchNewResource(tmpFolder.newFile().getPath(), false),
        IsmRecordCoder.of(
            1,
            0,
            ImmutableList.<Coder<?>>of(ByteArrayCoder.of(), NON_DETERMINISTIC_CODER),
            ByteArrayCoder.of()),
        BLOOM_FILTER_SIZE_LIMIT);
  }
}
