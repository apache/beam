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
package org.apache.beam.examples.multilanguage;

import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class WasmTest {

  void runExample(WasmTestOptions options, String expansionService) {
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> data = pipeline.apply(Create.of("aaa", "bbb", "ccc", "ddd", "eee"))
        .apply(External.of("beam:transform:add:1.0", new byte[]{}, expansionService));
    data.apply("WriteCounts", TextIO.write().to("gs://clouddfe-chamikara/wasabi_output/output"));

    pipeline.run().waitUntilFinish();
  }

  public interface WasmTestOptions extends PipelineOptions {

    @Description("Output path")
    String getOutput();

    void setOutput(String value);

    /** Set this option to specify Python expansion service URL. */
    @Description("URL of Python expansion service")
    String getExpansionService();

    void setExpansionService(String value);
  }

  public static void main(String[] args) {
    WasmTestOptions options = PipelineOptionsFactory.fromArgs(args).as(WasmTestOptions.class);
    WasmTest example = new WasmTest();
    example.runExample(options, options.getExpansionService());
  }
}
