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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for {@link AvroCoder}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
public class AvroCoderBenchmark {

  @DefaultCoder(AvroCoder.class)
  private static class Pojo {
    public String text;
    public int count;

    // Empty constructor required for Avro decoding.
    @SuppressWarnings("unused")
    public Pojo() {
    }

    public Pojo(String text, int count) {
      this.text = text;
      this.count = count;
    }

    // auto-generated
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Pojo pojo = (Pojo) o;

      if (count != pojo.count) {
        return false;
      }
      if (text != null
          ? !text.equals(pojo.text)
          : pojo.text != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String toString() {
      return "Pojo{"
          + "text='" + text + '\''
          + ", count=" + count
          + '}';
    }
  }

  AvroCoder<Pojo> coder = AvroCoder.of(Pojo.class);

  @Param({"true", "false"})
  boolean isWholeStream;

  Pojo shortPojo;
  Pojo longPojo;

  @Setup
  public void setUp() {
    shortPojo = new Pojo("hello world", 42);

    char[] bytes60k = new char[60 * 1024];
    Arrays.fill(bytes60k, 'a');
    longPojo = new Pojo(new String(bytes60k), 42);
  }

  @Benchmark
  public Pojo codeShortPojo() throws IOException {
    return CoderBenchmarking.testCoder(coder, isWholeStream, shortPojo);
  }

  @Benchmark
  public Pojo codeLongPojo() throws Exception {
    return CoderBenchmarking.testCoder(coder, isWholeStream, longPojo);
  }
}
