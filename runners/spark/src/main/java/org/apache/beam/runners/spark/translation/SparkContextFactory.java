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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

/**
 * The Spark context factory.
 */
public final class SparkContextFactory {

  /**
   * If the property {@code beam.spark.test.reuseSparkContext} is set to
   * {@code true} then the Spark context will be reused for beam pipelines.
   * This property should only be enabled for tests.
   */
  static final String TEST_REUSE_SPARK_CONTEXT = "beam.spark.test.reuseSparkContext";

  private static JavaSparkContext sparkContext;
  private static String sparkMaster;

  private SparkContextFactory() {
  }

  public static synchronized JavaSparkContext getSparkContext(String master, String appName) {
    if (Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT)) {
      if (sparkContext == null) {
        sparkContext = createSparkContext(master, appName);
        sparkMaster = master;
      } else if (!master.equals(sparkMaster)) {
        throw new IllegalArgumentException(String.format("Cannot reuse spark context "
                + "with different spark master URL. Existing: %s, requested: %s.",
            sparkMaster, master));
      }
      return sparkContext;
    } else {
      return createSparkContext(master, appName);
    }
  }

  static synchronized void stopSparkContext(JavaSparkContext context) {
    if (!Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT)) {
      context.stop();
    }
  }

  private static JavaSparkContext createSparkContext(String master, String appName) {
    SparkConf conf = new SparkConf();
    conf.setMaster(master);
    conf.setAppName(appName);
    conf.set("spark.serializer", KryoSerializer.class.getCanonicalName());
    return new JavaSparkContext(conf);
  }
}
