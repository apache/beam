/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import org.junit.Assert;
import org.junit.Test;

public class TestSparkPipelineOptionsFactory {
  @Test
  public void testDefaultCreateMethod() {
    SparkPipelineOptions actualOptions = SparkPipelineOptionsFactory.create();
    Assert.assertEquals("local[1]", actualOptions.getSparkMaster());
  }

  @Test
  public void testSettingCustomOptions() {
    SparkPipelineOptions actualOptions = SparkPipelineOptionsFactory.create();
    actualOptions.setSparkMaster("spark://207.184.161.138:7077");
    Assert.assertEquals("spark://207.184.161.138:7077", actualOptions.getSparkMaster());
  }
}
