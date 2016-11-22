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

import com.google.common.collect.Iterables;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * DStream holder Can also crate a DStream from a supplied queue of values, but mainly for testing.
 */
public class UnboundedDataset<T> implements Dataset {
  // only set if creating a DStream from a static collection
  @Nullable private transient JavaStreamingContext jssc;

  private Iterable<Iterable<T>> values;
  private Coder<T> coder;
  private JavaDStream<WindowedValue<T>> dStream;

  UnboundedDataset(JavaDStream<WindowedValue<T>> dStream) {
    this.dStream = dStream;
  }

  public UnboundedDataset(Iterable<Iterable<T>> values, JavaStreamingContext jssc, Coder<T> coder) {
    this.values = values;
    this.jssc = jssc;
    this.coder = coder;
  }

  @SuppressWarnings("ConstantConditions")
  JavaDStream<WindowedValue<T>> getDStream() {
    if (dStream == null) {
      WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      // create the DStream from queue
      Queue<JavaRDD<WindowedValue<T>>> rddQueue = new LinkedBlockingQueue<>();
      JavaRDD<WindowedValue<T>> lastRDD = null;
      for (Iterable<T> v : values) {
        Iterable<WindowedValue<T>> windowedValues =
            Iterables.transform(v, WindowingHelpers.<T>windowValueFunction());
        JavaRDD<WindowedValue<T>> rdd = jssc.sc().parallelize(
            CoderHelpers.toByteArrays(windowedValues, windowCoder)).map(
            CoderHelpers.fromByteFunction(windowCoder));
        rddQueue.offer(rdd);
        lastRDD = rdd;
      }
      // create DStream from queue, one at a time,
      // with last as default in case batches repeat (graceful stops for example).
      // if the stream is empty, avoid creating a default empty RDD.
      // mainly for unit test so no reason to have this configurable.
      dStream = lastRDD != null ? jssc.queueStream(rddQueue, true, lastRDD)
          : jssc.queueStream(rddQueue, true);
    }
    return dStream;
  }

  @Override
  public void cache() {
    dStream.cache();
  }

  @Override
  public void action() {
    dStream.foreachRDD(new VoidFunction<JavaRDD<WindowedValue<T>>>() {
      @Override
      public void call(JavaRDD<WindowedValue<T>> rdd) throws Exception {
        rdd.count();
      }
    });
  }

  @Override
  public void setName(String name) {
    // ignore
  }
}
