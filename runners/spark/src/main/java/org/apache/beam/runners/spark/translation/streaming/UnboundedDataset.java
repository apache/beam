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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DStream holder Can also crate a DStream from a supplied queue of values, but mainly for testing.
 */
public class UnboundedDataset<T> implements Dataset {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedDataset.class);
  private static final AtomicInteger queuedStreamIds = new AtomicInteger();

  // only set if creating a DStream from a static collection
  @Nullable private transient JavaStreamingContext jssc;

  private Iterable<Iterable<T>> values;
  private Coder<T> coder;
  private JavaDStream<WindowedValue<T>> dStream;
  // points to the input streams that created this UnboundedDataset,
  // should be greater > 1 in case of Flatten for example.
  // when using GlobalWatermarkHolder this information helps to take only the relevant watermarks
  // and reason about them accordingly.
  private final List<Integer> streamingSources = new ArrayList<>();

  public UnboundedDataset(JavaDStream<WindowedValue<T>> dStream, List<Integer> streamingSources) {
    this.dStream = dStream;
    this.streamingSources.addAll(streamingSources);
  }

  public UnboundedDataset(Iterable<Iterable<T>> values, JavaStreamingContext jssc, Coder<T> coder) {
    this.values = values;
    this.jssc = jssc;
    this.coder = coder;
    // QueuedStream will have a negative (decreasing) unique id.
    this.streamingSources.add(queuedStreamIds.decrementAndGet());
  }

  @VisibleForTesting
  public static void resetQueuedStreamIds() {
    queuedStreamIds.set(0);
  }

  @SuppressWarnings("ConstantConditions")
  JavaDStream<WindowedValue<T>> getDStream() {
    if (dStream == null) {
      WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      // create the DStream from queue
      Queue<JavaRDD<WindowedValue<T>>> rddQueue = new LinkedBlockingQueue<>();
      for (Iterable<T> v : values) {
        Iterable<WindowedValue<T>> windowedValues =
            Iterables.transform(v, WindowingHelpers.<T>windowValueFunction());
        JavaRDD<WindowedValue<T>> rdd = jssc.sc().parallelize(
            CoderHelpers.toByteArrays(windowedValues, windowCoder)).map(
            CoderHelpers.fromByteFunction(windowCoder));
        rddQueue.offer(rdd);
      }
      // create DStream from queue, one at a time.
      dStream = jssc.queueStream(rddQueue, true);
    }
    return dStream;
  }

  public List<Integer> getStreamingSources() {
    return streamingSources;
  }

  public void cache() {
    dStream.cache();
  }

  @Override
  public void cache(String storageLevel) {
    // we "force" MEMORY storage level in streaming
    LOG.warn("Provided StorageLevel ignored for stream, using default level");
    cache();
  }

  @Override
  public void action() {
    // Force computation of DStream.
    dStream.dstream().register();
  }

  @Override
  public void setName(String name) {
    // ignore
  }
}
