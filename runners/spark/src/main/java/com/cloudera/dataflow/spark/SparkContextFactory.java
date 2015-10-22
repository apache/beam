/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

final class SparkContextFactory {

  /**
   * If the property <code>dataflow.spark.test.reuseSparkContext</code> is set to
   * <code>true</code> then the Spark context will be reused for dataflow pipelines.
   * This property should only be enabled for tests.
   */
  public static final String TEST_REUSE_SPARK_CONTEXT =
      "dataflow.spark.test.reuseSparkContext";
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
        throw new IllegalArgumentException(String.format("Cannot reuse spark context " +
                "with different spark master URL. Existing: %s, requested: %s.",
            sparkMaster, master));
      }
      return sparkContext;
    } else {
      return createSparkContext(master, appName);
    }
  }

  public static synchronized void stopSparkContext(JavaSparkContext context) {
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
