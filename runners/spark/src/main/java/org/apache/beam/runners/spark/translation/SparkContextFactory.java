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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.SparkRunnerKryoRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SparkContextFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SparkContextFactory.class);

  /**
   * If the property {@code beam.spark.test.reuseSparkContext} is set to {@code true} then the Spark
   * context will be reused for beam pipelines. This property should only be enabled for tests.
   *
   * @deprecated This will leak your SparkContext, any attempt to create a new SparkContext later
   *     will fail. Please use {@link #setProvidedSparkContext(JavaSparkContext)} / {@link
   *     #clearProvidedSparkContext()} instead to properly control the lifecycle of your context.
   *     Alternatively you may also provide a SparkContext using {@link
   *     SparkContextOptions#setUsesProvidedSparkContext(boolean)} together with {@link
   *     SparkContextOptions#setProvidedSparkContext(JavaSparkContext)} and close that one
   *     appropriately. Tests of this module should use {@code SparkContextRule}.
   */
  @Deprecated
  public static final String TEST_REUSE_SPARK_CONTEXT = "beam.spark.test.reuseSparkContext";

  // Spark allows only one context for JVM so this can be static.
  private static @Nullable JavaSparkContext sparkContext;

  // Remember spark master if TEST_REUSE_SPARK_CONTEXT is enabled.
  private static @Nullable String reusableSparkMaster;

  // SparkContext is provided by the user instead of simply reused using TEST_REUSE_SPARK_CONTEXT
  private static boolean hasProvidedSparkContext;

  private SparkContextFactory() {}

  /**
   * Set an externally managed {@link JavaSparkContext} that will be used if {@link
   * SparkContextOptions#getUsesProvidedSparkContext()} is set to {@code true}.
   *
   * <p>A Spark context can also be provided using {@link
   * SparkContextOptions#setProvidedSparkContext(JavaSparkContext)}. However, it will be dropped
   * during serialization potentially leading to confusing behavior. This is particularly the case
   * when used in tests with {@link org.apache.beam.sdk.testing.TestPipeline}.
   */
  public static synchronized void setProvidedSparkContext(JavaSparkContext providedSparkContext) {
    sparkContext = checkNotNull(providedSparkContext);
    hasProvidedSparkContext = true;
    reusableSparkMaster = null;
  }

  public static synchronized void clearProvidedSparkContext() {
    hasProvidedSparkContext = false;
    sparkContext = null;
  }

  public static synchronized JavaSparkContext getSparkContext(SparkPipelineOptions options) {
    SparkContextOptions contextOptions = options.as(SparkContextOptions.class);
    if (contextOptions.getUsesProvidedSparkContext()) {
      JavaSparkContext jsc = contextOptions.getProvidedSparkContext();
      if (jsc != null) {
        setProvidedSparkContext(jsc);
      } else if (hasProvidedSparkContext) {
        jsc = sparkContext;
      }
      if (jsc == null) {
        throw new IllegalStateException(
            "No Spark context was provided. Use SparkContextFactor.setProvidedSparkContext to do so.");
      } else if (jsc.sc().isStopped()) {
        LOG.error("The provided Spark context " + jsc + " was already stopped.");
        throw new IllegalStateException("The provided Spark context was already stopped");
      }
      LOG.info("Using a provided Spark Context");
      return jsc;
    } else if (Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT)) {
      // This is highly discouraged as it leaks the SparkContext without any way to close it.
      // Attempting to create any new SparkContext later will fail.
      // If the context is null or stopped for some reason, re-create it.
      @Nullable JavaSparkContext jsc = sparkContext;
      if (jsc == null || jsc.sc().isStopped()) {
        sparkContext = jsc = createSparkContext(contextOptions);
        reusableSparkMaster = options.getSparkMaster();
        hasProvidedSparkContext = false;
      } else if (hasProvidedSparkContext) {
        throw new IllegalStateException(
            "Usage of provided Spark context is disabled in SparkPipelineOptions.");
      } else if (!options.getSparkMaster().equals(reusableSparkMaster)) {
        throw new IllegalStateException(
            String.format(
                "Cannot reuse spark context "
                    + "with different spark master URL. Existing: %s, requested: %s.",
                reusableSparkMaster, options.getSparkMaster()));
      }
      return jsc;
    } else {
      JavaSparkContext jsc = createSparkContext(contextOptions);
      clearProvidedSparkContext(); // any provided context can't be valid anymore
      return jsc;
    }
  }

  public static synchronized void stopSparkContext(JavaSparkContext context) {
    if (!Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT) && !hasProvidedSparkContext) {
      context.stop();
    }
  }

  private static JavaSparkContext createSparkContext(SparkPipelineOptions options) {
    LOG.info("Creating a brand new Spark Context.");
    SparkConf conf = new SparkConf();
    if (!conf.contains("spark.master")) {
      // set master if not set.
      conf.setMaster(options.getSparkMaster());
    }

    if (options.getFilesToStage() != null && !options.getFilesToStage().isEmpty()) {
      conf.setJars(options.getFilesToStage().toArray(new String[0]));
    }

    conf.setAppName(options.getAppName());
    // register immutable collections serializers because the SDK uses them.
    conf.set("spark.kryo.registrator", SparkRunnerKryoRegistrator.class.getName());
    return new JavaSparkContext(conf);
  }
}
