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
package org.apache.beam.runners.spark.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.spark.translation.SparkCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/** A set of functions to provide API compatibility between Spark 2 and Spark 3. */
public class SparkCompat {
  /**
   * Union of dStreams in the given StreamingContext.
   *
   * <p>This is required because the API to join (union) DStreams is different among Spark versions.
   * See https://issues.apache.org/jira/browse/SPARK-25737
   */
  public static <T> JavaDStream<WindowedValue<T>> joinStreams(
      JavaStreamingContext streamingContext, List<JavaDStream<WindowedValue<T>>> dStreams) {
    try {
      if (streamingContext.sparkContext().version().startsWith("3")) {
        // This invokes by reflection the equivalent of:
        // return streamingContext.union(
        //        JavaConverters.asScalaIteratorConverter(dStreams.iterator()).asScala().toSeq());
        Method method = streamingContext.getClass().getDeclaredMethod("union", JavaDStream[].class);
        Object result =
            method.invoke(streamingContext, new Object[] {dStreams.toArray(new JavaDStream[0])});
        return (JavaDStream<WindowedValue<T>>) result;
      }
      // This invokes by reflection the equivalent of:
      // return streamingContext.union(dStreams.remove(0), dStreams);
      Method method =
          streamingContext.getClass().getDeclaredMethod("union", JavaDStream.class, List.class);
      Object result = method.invoke(streamingContext, dStreams.remove(0), dStreams);
      return (JavaDStream<WindowedValue<T>>) result;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking Spark union", e);
    }
  }

  /**
   * Extracts the output for a given collection of WindowedAccumulators.
   *
   * <p>This is required because the API of JavaPairRDD.flatMapValues is different among Spark
   * versions. See https://issues.apache.org/jira/browse/SPARK-19287
   */
  public static <K, InputT, AccumT, OutputT> JavaPairRDD<K, WindowedValue<OutputT>> extractOutput(
      JavaPairRDD<K, SparkCombineFn.WindowedAccumulator<KV<K, InputT>, InputT, AccumT, ?>>
          accumulatePerKey,
      SparkCombineFn<KV<K, InputT>, InputT, AccumT, OutputT> sparkCombineFn) {
    try {
      if (accumulatePerKey.context().version().startsWith("3")) {
        FlatMapFunction<
                SparkCombineFn.WindowedAccumulator<KV<K, InputT>, InputT, AccumT, ?>,
                WindowedValue<OutputT>>
            flatMapFunction =
                (FlatMapFunction<
                        SparkCombineFn.WindowedAccumulator<KV<K, InputT>, InputT, AccumT, ?>,
                        WindowedValue<OutputT>>)
                    windowedAccumulator ->
                        sparkCombineFn.extractOutputStream(windowedAccumulator).iterator();
        // This invokes by reflection the equivalent of:
        // return accumulatePerKey.flatMapValues(flatMapFunction);
        Method method =
            accumulatePerKey.getClass().getDeclaredMethod("flatMapValues", FlatMapFunction.class);
        Object result = method.invoke(accumulatePerKey, flatMapFunction);
        return (JavaPairRDD<K, WindowedValue<OutputT>>) result;
      }

      Function<
              SparkCombineFn.WindowedAccumulator<KV<K, InputT>, InputT, AccumT, ?>,
              Iterable<WindowedValue<OutputT>>>
          flatMapFunction =
              windowedAccumulator ->
                  sparkCombineFn
                      .extractOutputStream(windowedAccumulator)
                      .collect(Collectors.toList());
      // This invokes by reflection the equivalent of:
      // return accumulatePerKey.flatMapValues(flatMapFunction);
      Method method =
          accumulatePerKey.getClass().getDeclaredMethod("flatMapValues", Function.class);
      Object result = method.invoke(accumulatePerKey, flatMapFunction);
      return (JavaPairRDD<K, WindowedValue<OutputT>>) result;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error invoking Spark flatMapValues", e);
    }
  }
}
