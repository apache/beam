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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for {@link StringUtf8Coder}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
public class StringUtf8CoderBenchmark {

  StringUtf8Coder coder = StringUtf8Coder.of();

  @Param({"true", "false"})
  boolean isWholeStream;

  String shortString;
  String longString;

  @Setup
  public void setUp() {
    shortString = "hello world";

    char[] bytes60k = new char[60 * 1024];
    Arrays.fill(bytes60k, 'a');
    longString = new String(bytes60k);
  }

  @Benchmark
  public String codeEmptyString() throws IOException {
    return CoderBenchmarking.testCoder(coder, isWholeStream, "");
  }

  @Benchmark
  public String codeShortString() throws IOException {
    return CoderBenchmarking.testCoder(coder, isWholeStream, shortString);
  }

  @Benchmark
  public String codeLongString() throws IOException {
    return CoderBenchmarking.testCoder(coder, isWholeStream, longString);
  }
}
