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

import java.util.List;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.UnsafeByteOperations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/** Benchmarks for {@link ByteStringOutputStream}. */
public class ByteStringOutputStreamBenchmark {

  private static final int MANY_WRITES = 10_000;
  private static final int FEW_WRITES = 5;
  private static final byte[] LARGE_BUFFER = new byte[1000];
  private static final byte[] SMALL_BUFFER = new byte[20];

  @State(Scope.Thread)
  public static class ProtobufByteStringOutputStream {
    final ByteString.Output output = ByteString.newOutput();

    @TearDown
    public void tearDown() throws Exception {
      output.close();
    }
  }

  @State(Scope.Thread)
  public static class SdkCoreByteStringOutputStream {
    final ByteStringOutputStream output = new ByteStringOutputStream();

    @TearDown
    public void tearDown() throws Exception {
      output.close();
    }
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamManyMixedWritesWithoutReuse() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < MANY_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * MANY_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamFewMixedWritesWithoutReuse() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < FEW_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamManyMixedWritesWithoutReuse() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < MANY_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * MANY_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamFewMixedWritesWithoutReuse() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < FEW_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamManyTinyWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(1);
    }
    if (output.toByteString().size() != MANY_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamManySmallWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(SMALL_BUFFER);
    }
    if (output.toByteString().size() != MANY_WRITES * SMALL_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamManyLargeWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(LARGE_BUFFER);
    }
    if (output.toByteString().size() != MANY_WRITES * LARGE_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamFewTinyWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(1);
    }
    if (output.toByteString().size() != FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamFewSmallWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(SMALL_BUFFER);
    }
    if (output.toByteString().size() != FEW_WRITES * SMALL_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamFewLargeWrites() throws Exception {
    ByteString.Output output = ByteString.newOutput();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(LARGE_BUFFER);
    }
    if (output.toByteString().size() != FEW_WRITES * LARGE_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamManyMixedWritesWithReuse(
      ProtobufByteStringOutputStream state) throws Exception {
    ByteString.Output output = state.output;
    for (int i = 0; i < 9850; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * 9850) {
      throw new IllegalArgumentException();
    }
    output.reset();
  }

  @Benchmark
  public void testProtobufByteStringOutputStreamFewMixedWritesWithReuse(
      ProtobufByteStringOutputStream state) throws Exception {
    ByteString.Output output = state.output;
    for (int i = 0; i < FEW_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteString().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.reset();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamManyTinyWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(1);
    }
    if (output.toByteString().size() != MANY_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamManySmallWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(SMALL_BUFFER);
    }
    if (output.toByteString().size() != MANY_WRITES * SMALL_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamManyLargeWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < MANY_WRITES; ++i) {
      output.write(LARGE_BUFFER);
    }
    if (output.toByteString().size() != MANY_WRITES * LARGE_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamFewTinyWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(1);
    }
    if (output.toByteString().size() != FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamFewSmallWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(SMALL_BUFFER);
    }
    if (output.toByteString().size() != FEW_WRITES * SMALL_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamFewLargeWrites() throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (int i = 0; i < FEW_WRITES; ++i) {
      output.write(LARGE_BUFFER);
    }
    if (output.toByteString().size() != FEW_WRITES * LARGE_BUFFER.length) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamManyMixedWritesWithReuse(
      SdkCoreByteStringOutputStream state) throws Exception {
    ByteStringOutputStream output = state.output;
    for (int i = 0; i < 9850; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteStringAndReset().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * 9850) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  @Benchmark
  public void testSdkCoreByteStringOutputStreamFewMixedWritesWithReuse(
      SdkCoreByteStringOutputStream state) throws Exception {
    ByteStringOutputStream output = state.output;
    for (int i = 0; i < FEW_WRITES; i++) {
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
      output.write(LARGE_BUFFER);
      output.write(1);
      output.write(SMALL_BUFFER);
      output.write(1);
    }
    if (output.toByteStringAndReset().size()
        != (4 + 2 * SMALL_BUFFER.length + LARGE_BUFFER.length) * FEW_WRITES) {
      throw new IllegalArgumentException();
    }
    output.close();
  }

  /**
   * These benchmarks below provide good details as to the cost of creating a new buffer vs copying
   * a subset of the existing one and re-using the larger one.
   */
  public static class NewVsCopy {
    @State(Scope.Thread)
    public static class ArrayCopyState {
      @Param({
        "512/1024", "640/1024", "768/1024", "896/1024",
        "4096/8192", "5120/8192", "6144/8192", "7168/8192",
        "20480/65536", "24576/65536", "28672/65536", "32768/65536",
        "131072/262144", "163840/262144", "196608/262144", "229376/262144",
        "524288/1048576", "655360/1048576", "786432/1048576", "917504/1048576"
      })
      String copyVsNew;

      int copyThreshold;
      int byteArraySize;
      public byte[] src;

      @Setup
      public void setup() {
        List<String> parts = Splitter.on('/').splitToList(copyVsNew);
        copyThreshold = Integer.parseInt(parts.get(0));
        byteArraySize = Integer.parseInt(parts.get(1));
        src = new byte[byteArraySize];
      }
    }

    @Benchmark
    public void testCopyArray(ArrayCopyState state, Blackhole bh) {
      byte[] dest = new byte[state.copyThreshold];
      System.arraycopy(state.src, 0, dest, 0, state.copyThreshold);
      bh.consume(UnsafeByteOperations.unsafeWrap(dest));
    }

    @State(Scope.Benchmark)
    public static class ArrayNewState {
      @Param({"1024", "8192", "65536", "262144", "1048576"})
      int byteArraySize;

      public byte[] src;

      @Setup
      public void setup() {
        src = new byte[byteArraySize];
      }
    }

    @Benchmark
    public void testNewArray(ArrayNewState state, Blackhole bh) {
      bh.consume(UnsafeByteOperations.unsafeWrap(state.src, 0, state.byteArraySize));
      state.src = new byte[state.byteArraySize];
    }
  }
}
