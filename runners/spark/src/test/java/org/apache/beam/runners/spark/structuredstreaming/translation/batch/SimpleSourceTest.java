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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.runners.spark.structuredstreaming.utils.SerializationDebugger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for beam to spark source translation. */
@RunWith(JUnit4.class)
public class SimpleSourceTest implements Serializable {
  private static Pipeline pipeline;
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setTestMode(true);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testSerialization() throws IOException {
    BoundedSource<Integer> source =
        new BoundedSource<Integer>() {

          @Override
          public List<? extends BoundedSource<Integer>> split(
              long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
            return new ArrayList<>();
          }

          @Override
          public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
            return 0;
          }

          @Override
          public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
            return null;
          }
        };
    String serializedSource = Base64Serializer.serializeUnchecked(source);
    Map<String, String> dataSourceOptions = new HashMap<>();
    dataSourceOptions.put(DatasetSourceBatch.BEAM_SOURCE_OPTION, serializedSource);
    dataSourceOptions.put(DatasetSourceBatch.DEFAULT_PARALLELISM, "4");
    dataSourceOptions.put(
        DatasetSourceBatch.PIPELINE_OPTIONS,
        new SerializablePipelineOptions(pipeline.getOptions()).toString());
    DataSourceReader objectToTest =
        new DatasetSourceBatch().createReader(new DataSourceOptions(dataSourceOptions));
    SerializationDebugger.testSerialization(objectToTest, TEMPORARY_FOLDER.newFile());
  }

  @Test
  public void testBoundedSource() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    PAssert.that(input).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }
}
