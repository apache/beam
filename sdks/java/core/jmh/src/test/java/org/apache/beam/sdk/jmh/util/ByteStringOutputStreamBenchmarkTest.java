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
package org.apache.beam.sdk.jmh.util;

import org.apache.beam.sdk.jmh.util.ByteStringOutputStreamBenchmark.NewVsCopy.ArrayCopyState;
import org.apache.beam.sdk.jmh.util.ByteStringOutputStreamBenchmark.NewVsCopy.ArrayNewState;
import org.apache.beam.sdk.jmh.util.ByteStringOutputStreamBenchmark.ProtobufByteStringOutputStream;
import org.apache.beam.sdk.jmh.util.ByteStringOutputStreamBenchmark.SdkCoreByteStringOutputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jmh.infra.Blackhole;

/** Tests for {@link ByteStringOutputStreamBenchmark}. */
@RunWith(JUnit4.class)
public class ByteStringOutputStreamBenchmarkTest {
  @Test
  public void testProtobufByteStringOutputStream() throws Exception {
    new ByteStringOutputStreamBenchmark()
        .testProtobufByteStringOutputStreamFewMixedWritesWithoutReuse();
    new ByteStringOutputStreamBenchmark()
        .testProtobufByteStringOutputStreamFewMixedWritesWithReuse(
            new ProtobufByteStringOutputStream());
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamFewLargeWrites();
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamFewSmallWrites();
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamFewTinyWrites();
    new ByteStringOutputStreamBenchmark()
        .testProtobufByteStringOutputStreamManyMixedWritesWithoutReuse();
    new ByteStringOutputStreamBenchmark()
        .testProtobufByteStringOutputStreamManyMixedWritesWithReuse(
            new ProtobufByteStringOutputStream());
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamManyLargeWrites();
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamManySmallWrites();
    new ByteStringOutputStreamBenchmark().testProtobufByteStringOutputStreamManyTinyWrites();
  }

  @Test
  public void testSdkCoreByteStringOutputStream() throws Exception {
    new ByteStringOutputStreamBenchmark()
        .testSdkCoreByteStringOutputStreamFewMixedWritesWithoutReuse();
    new ByteStringOutputStreamBenchmark()
        .testSdkCoreByteStringOutputStreamFewMixedWritesWithReuse(
            new SdkCoreByteStringOutputStream());
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamFewLargeWrites();
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamFewSmallWrites();
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamFewTinyWrites();
    new ByteStringOutputStreamBenchmark()
        .testSdkCoreByteStringOutputStreamManyMixedWritesWithoutReuse();
    new ByteStringOutputStreamBenchmark()
        .testSdkCoreByteStringOutputStreamManyMixedWritesWithReuse(
            new SdkCoreByteStringOutputStream());
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamManyLargeWrites();
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamManySmallWrites();
    new ByteStringOutputStreamBenchmark().testSdkCoreByteStringOutputStreamManyTinyWrites();
  }

  @Test
  public void testNewVsCopy() throws Exception {
    Blackhole bh =
        new Blackhole(
            "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    ArrayCopyState copyState = new ArrayCopyState();
    copyState.copyVsNew = "512/2048";
    copyState.setup();

    ArrayNewState newState = new ArrayNewState();
    newState.byteArraySize = 2048;
    newState.setup();

    new ByteStringOutputStreamBenchmark.NewVsCopy().testCopyArray(copyState, bh);
    new ByteStringOutputStreamBenchmark.NewVsCopy().testNewArray(newState, bh);
  }
}
