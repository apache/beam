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
package ${package};

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An example that counts words in Shakespeare, using Java 8 language features.
 *
 * <p>See {@link MinimalWordCount} for a comprehensive explanation.
 */
public class MinimalWordCountJava8 {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.from("gs://apache-beam-samples/shakespeare/*"))
     .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
         .withOutputType(TypeDescriptors.strings()))
     .apply(Filter.by((String word) -> !word.isEmpty()))
     .apply(Count.<String>perElement())
     .apply(MapElements
         .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue())
         .withOutputType(TypeDescriptors.strings()))

     // CHANGE 3/3: The Google Cloud Storage path is required for outputting the results to.
     .apply(TextIO.Write.to("gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX"));

    p.run().waitUntilFinish();
  }
}
