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

import org.apache.beam.runners.spark.coders.ImmutablesRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

/**
 * The Spark context factory.
 */
public final class SparkSessionFactory {

  /**
   * If the property {@code beam.spark.test.reuseSparkContext} is set to
   * {@code true} then the Spark context will be reused for beam pipelines.
   * This property should only be enabled for tests.
   */
  static final String TEST_REUSE_SPARK_CONTEXT = "beam.spark.test.reuseSparkContext";

  private SparkSessionFactory() {
  }

  public static SparkSession createSparkSession(String master, String appName) {
    return create(master, appName, defaultConfig());
  }

  private static synchronized SparkSession create(String master, String appName, SparkConf conf) {
    // SparkSession supports reuse with getOrCreate()
    return SparkSession.builder()
        .master(master)
        .appName(appName)
        .config(conf)
        .getOrCreate();
  }

  static synchronized void stopSparkSession(SparkSession session) {
    // avoid stopping the context when reuse is wanted.
    if (!Boolean.getBoolean(TEST_REUSE_SPARK_CONTEXT)) {
      session.stop();
    }
  }

  private static SparkConf defaultConfig() {
    SparkConf conf = new SparkConf();
    conf.set("spark.serializer", KryoSerializer.class.getCanonicalName());
    // register immutable collections serializers because the SDK uses them and they will be
    // used by Dataset Encoders
    conf.set("spark.kryo.registrator", ImmutablesRegistrator.class.getCanonicalName());
    return conf;
  }
}
