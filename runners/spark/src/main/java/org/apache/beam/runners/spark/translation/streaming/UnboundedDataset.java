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
package org.apache.beam.runners.spark.translation.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DStream holder Can also crate a DStream from a supplied queue of values, but mainly for testing.
 */
public class UnboundedDataset<T> implements Dataset {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedDataset.class);

  private JavaDStream<WindowedValue<T>> dStream;
  // points to the input streams that created this UnboundedDataset,
  // should be greater > 1 in case of Flatten for example.
  // when using GlobalWatermarkHolder this information helps to take only the relevant watermarks
  // and reason about them accordingly.
  private final List<Integer> streamSources = new ArrayList<>();

  public UnboundedDataset(JavaDStream<WindowedValue<T>> dStream, List<Integer> streamSources) {
    this.dStream = dStream;
    this.streamSources.addAll(streamSources);
  }

  public JavaDStream<WindowedValue<T>> getDStream() {
    return dStream;
  }

  public List<Integer> getStreamSources() {
    return streamSources;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void cache(String storageLevel, Coder<?> coder) {
    // we "force" MEMORY storage level in streaming
    if (!StorageLevel.fromString(storageLevel).equals(StorageLevel.MEMORY_ONLY_SER())) {
      LOG.warn(
          "Provided StorageLevel: {} is ignored for streams, using the default level: {}",
          storageLevel,
          StorageLevel.MEMORY_ONLY_SER());
    }
    // Caching can cause Serialization, we need to code to bytes
    // more details in https://issues.apache.org/jira/browse/BEAM-2669
    Coder<WindowedValue<T>> wc = (Coder<WindowedValue<T>>) coder;
    this.dStream =
        dStream.map(CoderHelpers.toByteFunction(wc)).cache().map(CoderHelpers.fromByteFunction(wc));
  }

  @Override
  public void action() {
    // Force computation of DStream.
    dStream.foreachRDD(rdd -> rdd.foreach(TranslationUtils.<WindowedValue<T>>emptyVoidFunction()));
  }

  @Override
  public void setName(String name) {
    // ignore
  }
}
