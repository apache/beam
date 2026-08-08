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
package org.apache.beam.sdk.jmh.coders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class RowCoderBenchmark {
  @State(Scope.Thread)
  public static class CoderState {
    @Param({"false", "true"})
    boolean staticEncoding;

    RowCoder coder;
    Row row;
    ByteArrayOutputStream output;
    ByteArrayInputStream input;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      Schema.Builder builder =
          Schema.builder().addByteField("_pythonsdk_any_type_byte").addByteArrayField("payload");
      if (staticEncoding) {
        builder.setOptions(
            Schema.Options.builder()
                .setOption("beam:option:row:static_encoding", FieldType.BOOLEAN, true)
                .build());
      }
      Schema schema = builder.build();
      coder = RowCoder.of(schema);
      row = Row.withSchema(schema).addValues((byte) 5, new byte[] {1, 2, 3, 4}).build();
      output = new ByteArrayOutputStream(64);
      coder.encode(row, output);
      input = new ByteArrayInputStream(output.toByteArray());
    }
  }

  @Benchmark
  public int encode(CoderState state) throws Exception {
    state.output.reset();
    state.coder.encode(state.row, state.output);
    return state.output.size();
  }

  @Benchmark
  public Row decode(CoderState state) throws Exception {
    state.input.reset();
    return state.coder.decode(state.input);
  }
}
