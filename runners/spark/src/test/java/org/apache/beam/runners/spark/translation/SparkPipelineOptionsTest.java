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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Simple test on the Spark runner pipeline options.
 */
public class SparkPipelineOptionsTest {
  @Test
  public void testDefaultCreateMethod() {
    SparkPipelineOptions actualOptions = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    Assert.assertEquals("local[1]", actualOptions.getSparkMaster());
  }

  @Test
  public void testSettingCustomOptions() {
    SparkPipelineOptions actualOptions = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    actualOptions.setSparkMaster("spark://207.184.161.138:7077");
    Assert.assertEquals("spark://207.184.161.138:7077", actualOptions.getSparkMaster());
  }
}
