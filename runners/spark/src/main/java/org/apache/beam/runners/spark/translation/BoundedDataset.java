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
package org.apache.beam.runners.spark.translation;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Holds an RDD or values for deferred conversion to an RDD if needed. PCollections are sometimes
 * created from a collection of objects (using RDD parallelize) and then only used to create View
 * objects; in which case they do not need to be converted to bytes since they are not transferred
 * across the network until they are broadcast.
 */
public class BoundedDataset<T> implements Dataset {
  // only set if creating an RDD from a static collection
  private transient @Nullable JavaSparkContext jsc;

  private Iterable<WindowedValue<T>> windowedValues;
  private Coder<T> coder;
  private JavaRDD<WindowedValue<T>> rdd;
  private List<byte[]> clientBytes;

  BoundedDataset(JavaRDD<WindowedValue<T>> rdd) {
    this.rdd = rdd;
  }

  BoundedDataset(Iterable<T> values, JavaSparkContext jsc, Coder<T> coder) {
    this.windowedValues = Iterables.transform(values, WindowedValue::valueInGlobalWindow);
    this.jsc = jsc;
    this.coder = coder;
  }

  @SuppressWarnings("ConstantConditions")
  public JavaRDD<WindowedValue<T>> getRDD() {
    if (rdd == null) {
      WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      rdd =
          jsc.parallelize(CoderHelpers.toByteArrays(windowedValues, windowCoder))
              .map(CoderHelpers.fromByteFunction(windowCoder));
    }
    return rdd;
  }

  List<byte[]> getBytes(WindowedValue.WindowedValueCoder<T> wvCoder) {
    if (clientBytes == null) {
      JavaRDDLike<byte[], ?> bytesRDD = rdd.map(CoderHelpers.toByteFunction(wvCoder));
      clientBytes = bytesRDD.collect();
    }
    return clientBytes;
  }

  Iterable<WindowedValue<T>> getValues(PCollection<T> pcollection) {
    if (windowedValues == null) {
      WindowFn<?, ?> windowFn = pcollection.getWindowingStrategy().getWindowFn();
      Coder<? extends BoundedWindow> windowCoder = windowFn.windowCoder();
      final WindowedValue.WindowedValueCoder<T> windowedValueCoder;
      if (windowFn instanceof GlobalWindows) {
        windowedValueCoder = WindowedValue.ValueOnlyWindowedValueCoder.of(pcollection.getCoder());
      } else {
        windowedValueCoder =
            WindowedValue.FullWindowedValueCoder.of(pcollection.getCoder(), windowCoder);
      }
      JavaRDDLike<byte[], ?> bytesRDD = rdd.map(CoderHelpers.toByteFunction(windowedValueCoder));
      List<byte[]> clientBytes = bytesRDD.collect();
      windowedValues =
          clientBytes.stream()
              .map(bytes -> CoderHelpers.fromByteArray(bytes, windowedValueCoder))
              .collect(Collectors.toList());
    }
    return windowedValues;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void cache(String storageLevel, Coder<?> coder) {
    StorageLevel level = StorageLevel.fromString(storageLevel);
    if (TranslationUtils.canAvoidRddSerialization(level)) {
      // if it is memory only reduce the overhead of moving to bytes
      this.rdd = getRDD().persist(level);
    } else {
      // Caching can cause Serialization, we need to code to bytes
      // more details in https://issues.apache.org/jira/browse/BEAM-2669
      Coder<WindowedValue<T>> windowedValueCoder = (Coder<WindowedValue<T>>) coder;
      this.rdd =
          getRDD()
              .map(v -> ValueAndCoderLazySerializable.of(v, windowedValueCoder))
              .persist(level)
              .map(v -> v.getOrDecode(windowedValueCoder));
    }
  }

  @Override
  public void action() {
    // Empty function to force computation of RDD.
    rdd.foreach(TranslationUtils.emptyVoidFunction());
  }

  @Override
  public void setName(String name) {
    getRDD().setName(name);
  }
}
