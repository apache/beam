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
package org.apache.beam.runners.spark.structuredstreaming;

import static java.util.stream.Collectors.toMap;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.fun1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.translation.SparkSessionFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.SparkSession;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SparkSessionRule extends ExternalResource implements Serializable {
  private transient SparkSession.Builder builder;
  private transient @Nullable SparkSession session = null;

  public SparkSessionRule(String sparkMaster, Map<String, String> sparkConfig) {
    builder = SparkSessionFactory.sessionBuilder(sparkMaster);
    sparkConfig.forEach(builder::config);
  }

  public SparkSessionRule(KV<String, String>... sparkConfig) {
    this("local[2]", sparkConfig);
  }

  public SparkSessionRule(String sparkMaster, KV<String, String>... sparkConfig) {
    this(sparkMaster, Arrays.stream(sparkConfig).collect(toMap(KV::getKey, KV::getValue)));
  }

  public SparkSession getSession() {
    if (session == null) {
      throw new IllegalStateException("SparkSession not available");
    }
    return session;
  }

  public PipelineOptions createPipelineOptions() {
    return configure(TestPipeline.testingPipelineOptions());
  }

  public PipelineOptions configure(PipelineOptions options) {
    SparkStructuredStreamingPipelineOptions opts =
        options.as(SparkStructuredStreamingPipelineOptions.class);
    opts.setUseActiveSparkSession(true);
    opts.setRunner(SparkStructuredStreamingRunner.class);
    opts.setTestMode(true);
    return opts;
  }

  /** {@code true} if sessions contains cached Datasets or RDDs. */
  public boolean hasCachedData() {
    return !session.sharedState().cacheManager().isEmpty()
        || !session.sparkContext().getPersistentRDDs().isEmpty();
  }

  public TestRule clearCache() {
    return new ExternalResource() {
      @Override
      protected void after() {
        // clear cached datasets
        session.sharedState().cacheManager().clearCache();
        // clear cached RDDs
        session.sparkContext().getPersistentRDDs().foreach(fun1(t -> t._2.unpersist(true)));
      }
    };
  }

  @Override
  public Statement apply(Statement base, Description description) {
    builder.appName(description.getDisplayName());
    return super.apply(base, description);
  }

  @Override
  protected void before() throws Throwable {
    session = builder.getOrCreate();
  }

  @Override
  protected void after() {
    getSession().stop();
    session = null;
  }
}
