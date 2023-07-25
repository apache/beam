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
package org.apache.beam.sdk.jmh.schemas;

import org.apache.beam.sdk.jmh.schemas.RowBundles.ArrayOfNestedStringBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.ArrayOfStringBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.ByteBufferBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.BytesBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.DateTimeBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.IntBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.MapOfIntBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.MapOfNestedIntBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.NestedBytesBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.NestedIntBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.StringBuilderBundle;
import org.apache.beam.sdk.jmh.schemas.RowBundles.StringBundle;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks for {@link GetterBasedSchemaProvider} on reading / writing fields based on {@link
 * GetterBasedSchemaProvider#toRowFunction(TypeDescriptor) toRowFunction} / {@link
 * GetterBasedSchemaProvider#fromRowFunction(TypeDescriptor) fromRowFunction}.
 *
 * <p>Each benchmark method invocation, depending on {@link RowBundle#action}, either reads a single
 * field of a bundle of {@link RowBundle#bundleSize n} rows using the corresponding getter via
 * {@link RowWithGetters#getValue} or writes that field using the corresponding setter to a new
 * object instance.
 *
 * <p>Rows are created upfront and provided as JMH {@link State} to exclude initialization costs
 * from the measurement.
 *
 * <ul>
 *   <li>The score doesn't reflect read / write access only, measurement includes iterating over a
 *       large number of rows.
 *   <li>All rows contain just a single field. Nevertheless it is tricky to compare scores between
 *       different benchmarks: nested structures are read recursively and collections / maps are
 *       iterated through.
 * </ul>
 */
public class GetterBasedSchemaProviderBenchmark {
  @Benchmark
  public void processIntField(IntBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processNestedIntField(NestedIntBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processStringField(StringBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processStringBuilderField(StringBuilderBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processDateTimeField(DateTimeBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processBytesField(BytesBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processNestedBytesField(NestedBytesBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processByteBufferField(ByteBufferBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processArrayOfStringField(ArrayOfStringBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processArrayOfNestedStringField(ArrayOfNestedStringBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processMapOfIntField(MapOfIntBundle state, Blackhole bh) {
    state.processRows(bh);
  }

  @Benchmark
  public void processMapOfNestedIntField(MapOfNestedIntBundle state, Blackhole bh) {
    state.processRows(bh);
  }
}
