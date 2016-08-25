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

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Spark context factory.
 */
public final class SparkContextFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SparkContextFactory.class);

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

  public static synchronized JavaSparkContext getSparkContext(SparkPipelineOptions options) {
    // reuse should be ignored if the context is provided.
    if (Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT) && !options.getUsesProvidedSparkContext()) {
      if (sparkContext == null) {
        sparkContext = createSparkContext(options);
        sparkMaster = options.getSparkMaster();
      } else if (!options.getSparkMaster().equals(sparkMaster)) {
        throw new IllegalArgumentException(String.format("Cannot reuse spark context "
            + "with different spark master URL. Existing: %s, requested: %s.",
                sparkMaster, options.getSparkMaster()));
      }
      return sparkContext;
    } else {
      return createSparkContext(options);
    }
  }

  static synchronized void stopSparkContext(JavaSparkContext context) {
    if (!Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT)) {
      context.stop();
    }
  }

  private static JavaSparkContext createSparkContext(SparkPipelineOptions options) {
    if (options.getUsesProvidedSparkContext()) {
      LOG.info("Using a provided Spark Context");
      JavaSparkContext jsc = options.getProvidedSparkContext();
      if (jsc == null || jsc.sc().isStopped()){
        LOG.error("The provided Spark context " + jsc + " was not created or was stopped");
        throw new RuntimeException("The provided Spark context was not created or was stopped");
      }
      return jsc;
    } else {
      LOG.info("Creating a brand new Spark Context.");
      SparkConf conf = new SparkConf();
      if (!conf.contains("spark.master")) {
        // set master if not set.
        conf.setMaster(options.getSparkMaster());
      }
      conf.setAppName(options.getAppName());
      conf.set("spark.serializer", KryoSerializer.class.getCanonicalName());
      return new JavaSparkContext(conf);
    }
  }
}
