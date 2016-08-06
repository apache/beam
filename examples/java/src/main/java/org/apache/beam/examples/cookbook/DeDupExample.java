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
package org.apache.beam.examples.cookbook;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.RemoveDuplicates;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * This example uses as input Shakespeare's plays as plaintext files, and will remove any
 * duplicate lines across all the files. (The output does not preserve any input order).
 *
 * <p>Concepts: the RemoveDuplicates transform, and how to wire transforms together.
 * Demonstrates {@link org.apache.beam.sdk.io.TextIO.Read}/
 * {@link RemoveDuplicates}/{@link org.apache.beam.sdk.io.TextIO.Write}.
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 *   --project=YOUR_PROJECT_ID
 * and a local output file or output prefix on GCS:
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=BlockingDataflowRunner
 * and an output prefix on GCS:
 *   --output=gs://YOUR_OUTPUT_PREFIX
 *
 * <p>The input defaults to {@code gs://dataflow-samples/shakespeare/*} and can be
 * overridden with {@code --input}.
 */
public class DeDupExample {

  /**
   * Options supported by {@link DeDupExample}.
   *
   * <p>Inherits standard configuration options.
   */
  private static interface Options extends PipelineOptions {
    @Description("Path to the directory or GCS prefix containing files to read from")
    @Default.String("gs://dataflow-samples/shakespeare/*")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    /** Returns gs://${TEMP_LOCATION}/"deduped.txt". */
    public static class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        if (options.getTempLocation() != null) {
          return GcsPath.fromUri(options.getTempLocation())
              .resolve("deduped.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --tempLocation");
        }
      }
    }
  }


  public static void main(String[] args)
      throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.Read.from(options.getInput()))
     .apply(RemoveDuplicates.<String>create())
     .apply("DedupedShakespeare", TextIO.Write.to(options.getOutput()));

    p.run();
  }
}
