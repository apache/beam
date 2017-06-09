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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;


/**
 * Test the RDD storage level defined by user.
 */
public class StorageLevelTest {

  private static String beamTestPipelineOptions;

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void init() {
    beamTestPipelineOptions =
        System.getProperty(TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);

    System.setProperty(
        TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
        beamTestPipelineOptions.replace("]", ", \"--storageLevel=DISK_ONLY\"]"));
  }

  @AfterClass
  public static void teardown() {
    System.setProperty(
        TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS,
        beamTestPipelineOptions);
  }

  @Test
  public void test() throws Exception {
    PCollection<String> pCollection = pipeline.apply("CreateFoo", Create.of("foo"));

    // by default, the Spark runner doesn't cache the RDD if it accessed only one time.
    // So, to "force" the caching of the RDD, we have to call the RDD at least two time.
    // That's why we are using Count fn on the PCollection.
    pCollection.apply("CountAll", Count.<String>globally());

    PCollection<String> output = pCollection.apply(new StorageLevelPTransform());

    PAssert.thatSingleton(output).isEqualTo("Disk Serialized 1x Replicated");

    pipeline.run();
  }

}
