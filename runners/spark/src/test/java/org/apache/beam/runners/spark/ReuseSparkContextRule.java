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

import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.junit.rules.ExternalResource;

/**
 * Explicitly set {@link org.apache.spark.SparkContext} to be reused (or not) in tests.
 */
public class ReuseSparkContext extends ExternalResource {

  private final boolean reuse;

  private ReuseSparkContext(boolean reuse) {
    this.reuse = reuse;
  }

  public static ReuseSparkContext no() {
    return new ReuseSparkContext(false);
  }

  public static ReuseSparkContext yes() {
    return new ReuseSparkContext(true);
  }

  @Override
  protected void before() throws Throwable {
    System.setProperty(SparkContextFactory.TEST_REUSE_SPARK_CONTEXT, Boolean.toString(reuse));
  }
}
