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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

  /**
   * Gets active {@link SparkSession} or creates one using {@link
   * SparkStructuredStreamingPipelineOptions}.
   */
  public static SparkSession getOrCreateSession(SparkStructuredStreamingPipelineOptions options) {
    if (options.getUseActiveSparkSession()) {
      return SparkSession.active();
    }
    return sessionBuilder(options.getSparkMaster(), options.getAppName(), options.getFilesToStage())
        .getOrCreate();
  }

  /** Creates Spark session builder with some optimizations for local mode, e.g. in tests. */
  public static SparkSession.Builder sessionBuilder(String master) {
    return sessionBuilder(master, null, null);
  }

  private static SparkSession.Builder sessionBuilder(
      String master, @Nullable String appName, @Nullable List<String> jars) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(master);
    if (appName != null) {
      sparkConf.setAppName(appName);
    }
    if (jars != null && !jars.isEmpty()) {
      sparkConf.setJars(jars.toArray(new String[0]));
    }

    // By default, Spark defines 200 as a number of sql partitions. This seems too much for local
    // mode, so try to align with value of "sparkMaster" option in this case.
    // We should not overwrite this value (or any user-defined spark configuration value) if the
    // user has already configured it.
    if (master != null
        && master.startsWith("local[")
        && System.getProperty("spark.sql.shuffle.partitions") == null) {
      int numPartitions =
          Integer.parseInt(master.substring("local[".length(), master.length() - 1));
      if (numPartitions > 0) {
        sparkConf.set("spark.sql.shuffle.partitions", String.valueOf(numPartitions));
      }
    }
    return SparkSession.builder().config(sparkConf);
  }
}
