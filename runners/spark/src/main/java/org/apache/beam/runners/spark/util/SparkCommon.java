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

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.EventLoggingListener;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/** Common methods to build Spark specific objects used by different runners. */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SparkCommon {

  /**
   * Starts an EventLoggingListener to save Beam Metrics on Spark's History Server if event logging
   * is enabled.
   *
   * @return The associated EventLoggingListener or null if it could not be started.
   */
  public static @Nullable EventLoggingListener startEventLoggingListener(
      final JavaSparkContext jsc, SparkPipelineOptions pipelineOptions, long startTime) {
    EventLoggingListener eventLoggingListener = null;
    try {
      if (jsc.getConf().getBoolean("spark.eventLog.enabled", false)) {
        eventLoggingListener =
            new EventLoggingListener(
                jsc.getConf().getAppId(),
                scala.Option.apply("1"),
                new URI(jsc.getConf().get("spark.eventLog.dir", null)),
                jsc.getConf(),
                jsc.hadoopConfiguration());
        eventLoggingListener.initializeLogIfNecessary(false, false);
        eventLoggingListener.start();

        scala.collection.immutable.Map<String, String> logUrlMap =
            new scala.collection.immutable.HashMap<>();
        Tuple2<String, String>[] sparkMasters = jsc.getConf().getAllWithPrefix("spark.master");
        Tuple2<String, String>[] sparkExecutors =
            jsc.getConf().getAllWithPrefix("spark.executor.id");
        for (Tuple2<String, String> sparkExecutor : sparkExecutors) {
          eventLoggingListener.onExecutorAdded(
              new SparkListenerExecutorAdded(
                  startTime,
                  sparkExecutor._2(),
                  new ExecutorInfo(sparkMasters[0]._2(), 0, logUrlMap)));
        }
        return eventLoggingListener;
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "The URI syntax in the Spark config \"spark.eventLog.dir\" is not correct", e);
    }
    return eventLoggingListener;
  }
}
