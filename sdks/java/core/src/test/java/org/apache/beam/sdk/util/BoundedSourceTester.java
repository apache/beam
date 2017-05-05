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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Test utility that reads from a {@link BoundedSource}.
 */
public class BoundedSourceTester<T> {

  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 100;

  /**
   * Returns a {@link BoundedSourceTester} supporting unit-testing of the given
   * {@link BoundedSource}.
   */
  public static <T> BoundedSourceTester<T> of(BoundedSource<T> source) {
    return new BoundedSourceTester<>(source, PipelineOptionsFactory.create());
  }

  /**
   * Returns a copy of this {@link BoundedSourceTester} with the specified {@link PipelineOptions}.
   */
  public BoundedSourceTester<T> withOptions(PipelineOptions options) {
    return new BoundedSourceTester<>(this.source, options);
  }

  /**
   * Reads directly from the {@link BoundedSource}.
   */
  public List<T> read() throws IOException {
    return read(source, options);
  }

  /**
   * Initial splits then reads from the {@link BoundedSource}.
   */
  public List<T> initSplitThenRead() throws Exception {
    List<T> ret = Lists.newArrayList();
    List<? extends BoundedSource<T>> sources =
        source.splitIntoBundles(DEFAULT_BUNDLE_SIZE_BYTES, options);
    for (BoundedSource<T> s : sources) {
      ret.addAll(read(s, options));
    }
    return ret;
  }

  /**
   * Reads from the {@link BoundedSource} with dynamic splitting.
   */
  public List<T> readThenDynamicSplit() throws IOException {
    return readThenDynamicSplit(source, options);
  }

  /**
   * Initial splits then reads from the {@link BoundedSource} with dynamic splitting.
   */
  public List<T> initSplitThenReadThenDynamicSplit() throws Exception {
    List<T> ret = Lists.newArrayList();
    List<? extends BoundedSource<T>> sources =
        source.splitIntoBundles(DEFAULT_BUNDLE_SIZE_BYTES, options);
    for (BoundedSource<T> s : sources) {
      ret.addAll(readThenDynamicSplit(s, options));
    }
    return ret;
  }

  private static <T> List<T> read(BoundedSource<T> source, PipelineOptions options)
      throws IOException {
    List<T> ret = Lists.newArrayList();
    BoundedReader<T> reader = source.createReader(options);
    for (boolean available = reader.start(); available; available = reader.advance()) {
      ret.add(reader.getCurrent());
    }
    return ret;
  }

  private static <T> List<T> readThenDynamicSplit(
      BoundedSource<T> source, PipelineOptions options) throws IOException {
    List<T> ret = Lists.newArrayList();
    List<BoundedSource<T>> splitSources = Lists.newArrayList();
    BoundedReader<T> reader = source.createReader(options);
    for (boolean available = reader.start(); available; available = reader.advance()) {
      ret.add(reader.getCurrent());
      if (reader.getFractionConsumed() != null) {
        BoundedSource<T> rest = reader.splitAtFraction(reader.getFractionConsumed());
        if (rest != null) {
          splitSources.add(rest);
        }
      }
    }
    for (BoundedSource<T> s : splitSources) {
      ret.addAll(read(s, options));
    }
    return ret;
  }

  private final BoundedSource<T> source;
  private final PipelineOptions options;

  private BoundedSourceTester(BoundedSource<T> source, PipelineOptions options) {
    this.source = source;
    this.options = options;
  }
}
