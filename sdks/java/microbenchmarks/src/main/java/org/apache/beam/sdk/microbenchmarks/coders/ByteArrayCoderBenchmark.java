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
package org.apache.beam.sdk.microbenchmarks.coders;

import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for {@link ByteArrayCoder}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
public class ByteArrayCoderBenchmark {

  ByteArrayCoder coder = ByteArrayCoder.of();

  @Param({"true", "false"})
  boolean isWholeStream;

  byte[] shortArray;
  byte[] longArray;

  @Setup
  public void setUp() {
    shortArray = new byte[10];
    Arrays.fill(shortArray, (byte) 47);
    longArray = new byte[60 * 1024];
    Arrays.fill(longArray, (byte) 47);
  }

  @Benchmark
  public byte[] codeShortArray() throws IOException {
    return CoderBenchmarking.testCoder(coder, isWholeStream, shortArray);
  }

  @Benchmark
  public byte[] codeLongArray() throws Exception {
    return CoderBenchmarking.testCoder(coder, isWholeStream, longArray);
  }
}
