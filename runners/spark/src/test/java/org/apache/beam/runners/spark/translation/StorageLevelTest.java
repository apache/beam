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

import org.apache.beam.runners.spark.PipelineRule;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test the RDD storage level defined by user.
 */
public class StorageLevelTest {

  @Rule
  public final transient PipelineRule pipelineRule = PipelineRule.batch();

  @Test
  public void test() throws Exception {
    pipelineRule.getOptions().setStorageLevel("DISK_ONLY");
    Pipeline pipeline = pipelineRule.createPipeline();

    PCollection<String> pCollection = pipeline.apply(Create.of("foo"));

    // by default, the Spark runner doesn't cache the RDD if it accessed only one time.
    // So, to "force" the caching of the RDD, we have to call the RDD at least two time.
    // That's why we are using Count fn on the PCollection.
    pCollection.apply(Count.<String>globally());

    PCollection<String> output = pCollection.apply(new StorageLevelPTransform());

    PAssert.thatSingleton(output).isEqualTo("Disk Serialized 1x Replicated");

    pipeline.run();
  }

}
