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

import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink.SinkWriter;
import com.google.cloud.dataflow.sdk.values.KV;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Random;

/**
 * Tests for {@link IsmSink}.
 * Note that {@link IsmReaderTest} covers reading/writing tests. This tests
 * error cases for the {@link IsmSink}.
 */
@RunWith(JUnit4.class)
public class IsmSinkTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWriteOutOfOrderKeysIsError() throws Exception {
    IsmSink<byte[], byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), ByteArrayCoder.of(), ByteArrayCoder.of());
    SinkWriter<WindowedValue<KV<byte[], byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x01 }, new byte[0])));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x00 }, new byte[0])));
  }

  @Test
  public void testWriteEqualsKeysIsError() throws Exception {
    IsmSink<byte[], byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), ByteArrayCoder.of(), ByteArrayCoder.of());
    SinkWriter<WindowedValue<KV<byte[], byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x00 }, new byte[0])));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x00 }, new byte[0])));
  }

  @Test
  public void testWriteKeyWhichIsProperPrefixOfPreviousKeyIsError() throws Exception {
    IsmSink<byte[], byte[]> sink =
        new IsmSink<>(tmpFolder.newFile().getPath(), ByteArrayCoder.of(), ByteArrayCoder.of());
    SinkWriter<WindowedValue<KV<byte[], byte[]>>> sinkWriter = sink.writer();
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x00, 0x00 }, new byte[0])));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("expects keys to be written in strictly increasing order");
    sinkWriter.add(WindowedValue.valueInGlobalWindow(KV.of(new byte[]{ 0x00 }, new byte[0])));
  }

  @Test
  public void testWrite() throws Exception {
    Random random = new Random(23498321490L);
    for (int i : Arrays.asList(4, 8, 12)) {
      int minElements = (int) Math.pow(2, i);
      // Generates between 2^i and 2^(i + 1) elements.
      IsmReaderTest.runTestRead(
          IsmReaderTest.dataGenerator(minElements + random.nextInt(minElements),
              8 /* approximate key size */, 8 /* max value size */), tmpFolder.newFile());
    }
  }
}
