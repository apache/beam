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
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.io.TestStorageLevelPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Test the RDD storage level defined by user.
 */
public class StorageLevelTest {

  @Test
  public void test() throws Exception {
    SparkPipelineOptions pipelineOptions = PipelineOptionsFactory.create()
        .as(SparkPipelineOptions.class);
    pipelineOptions.setStorageLevel("DISK_ONLY");
    pipelineOptions.setRunner(SparkRunner.class);
    pipelineOptions.setEnableSparkMetricSinks(false);
    pipelineOptions.setStreaming(false);
    Pipeline p = Pipeline.create(pipelineOptions);

    PCollection<String> pCollection = p.apply(Create.of("foo"));

    pCollection.apply(Count.<String>globally());

    pCollection.apply(new TestStorageLevelPTransform());

    p.run();
  }

}
