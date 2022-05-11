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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.SparkSession;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SparkSessionRule extends ExternalResource implements Serializable {
  private transient SparkSession.Builder builder;
  private transient @Nullable SparkSession session = null;

  public SparkSessionRule(String sparkMaster, Map<String, String> sparkConfig) {
    builder = SparkSession.builder();
    sparkConfig.forEach(builder::config);
    builder.master(sparkMaster);
  }

  public SparkSessionRule(KV<String, String>... sparkConfig) {
    this("local", sparkConfig);
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
