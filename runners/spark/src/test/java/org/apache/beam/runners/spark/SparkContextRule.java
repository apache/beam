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
package org.apache.beam.runners.spark;

import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.ExternalResource;

public class SparkContextRule extends ExternalResource implements Serializable {
  private transient SparkConf sparkConf;
  private transient @Nullable JavaSparkContext sparkContext = null;

  public SparkContextRule(String sparkMaster, Map<String, String> sparkConfig) {
    sparkConf = new SparkConf();
    sparkConfig.forEach(sparkConf::set);
    sparkConf.setMaster(sparkMaster).setAppName(sparkMaster);
  }

  public SparkContextRule(KV<String, String>... sparkConfig) {
    this("local", sparkConfig);
  }

  public SparkContextRule(String sparkMaster, KV<String, String>... sparkConfig) {
    this(sparkMaster, Arrays.stream(sparkConfig).collect(toMap(KV::getKey, KV::getValue)));
  }

  public JavaSparkContext getSparkContext() {
    if (sparkContext == null) {
      throw new IllegalStateException("SparkContext not available");
    }
    return sparkContext;
  }

  public SparkContextOptions createPipelineOptions(
      Class<? extends PipelineRunner<?>> runner, String... options) {
    PipelineOptions opts = PipelineOptionsFactory.fromArgs(options).create();
    opts.as(SparkPipelineOptions.class).setRunner(runner);
    return configurePipelineOptions(opts);
  }

  public SparkContextOptions configurePipelineOptions(PipelineOptions opts) {
    SparkContextOptions ctxOpts = opts.as(SparkContextOptions.class);
    ctxOpts.setUsesProvidedSparkContext(true);
    ctxOpts.setProvidedSparkContext(sparkContext);
    return ctxOpts;
  }

  @Override
  protected void before() throws Throwable {
    sparkContext = new JavaSparkContext(sparkConf);
  }

  @Override
  protected void after() {
    getSparkContext().stop();
  }
}
