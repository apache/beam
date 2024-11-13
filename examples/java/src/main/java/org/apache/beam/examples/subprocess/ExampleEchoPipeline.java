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
package org.apache.beam.examples.subprocess;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.subprocess.kernel.SubProcessCommandLineArgs;
import org.apache.beam.examples.subprocess.kernel.SubProcessCommandLineArgs.Command;
import org.apache.beam.examples.subprocess.kernel.SubProcessKernel;
import org.apache.beam.examples.subprocess.utils.CallingSubProcessUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In this example batch pipeline we will invoke a simple Echo C++ library within a DoFn The sample
 * makes use of a ExternalLibraryDoFn class which abstracts the setup and processing of the
 * executable, logs and results. For this example we are using commands passed to the library based
 * on ordinal position but for a production system you should use a mechanism like ProtoBuffers with
 * Base64 encoding to pass the parameters to the library To test this example you will need to build
 * the files Echo.cc and EchoAgain.cc in a linux env matching the runner that you are using (using
 * g++ with static option). Once built copy them to the SourcePath defined in {@link
 * SubProcessPipelineOptions}
 */
public class ExampleEchoPipeline {

  public static void main(String[] args) throws Exception {

    // Read in the options for the pipeline
    SubProcessPipelineOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(SubProcessPipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    // Setup the Configuration option used with all transforms
    SubProcessConfiguration configuration = options.getSubProcessConfiguration();

    // Create some sample data to be fed to our c++ Echo library
    List<KV<String, String>> sampleData = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      String str = String.valueOf(i);
      sampleData.add(KV.of(str, str));
    }

    // Define the pipeline which is two transforms echoing the inputs out to Logs
    p.apply(Create.of(sampleData))
            .apply("Echo inputs round 1", ParDo.of(new EchoInputDoFn(configuration, "Echo")))
            .apply("Echo inputs round 2", ParDo.of(new EchoInputDoFn(configuration, "EchoAgain")));

    p.run();
  }

  /** Simple DoFn that echos the element, used as an example of running a C++ library. */
  @SuppressWarnings("serial")
  public static class EchoInputDoFn extends DoFn<KV<String, String>, KV<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(EchoInputDoFn.class);

    private SubProcessConfiguration configuration;
    private String binaryName;

    public EchoInputDoFn(SubProcessConfiguration configuration, String binary) {
      // Pass in configuration information the name of the filename of the sub-process and the level
      // of concurrency
      this.configuration = configuration;
      this.binaryName = binary;
    }

    @Setup
    public void setUp() throws Exception {
      CallingSubProcessUtils.setUp(configuration, binaryName);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        // Our Library takes a single command in position 0 which it will echo back in the result
        SubProcessCommandLineArgs commands = new SubProcessCommandLineArgs();
        Command command = new Command(0, String.valueOf(c.element().getValue()));
        commands.putCommand(command);

        // The ProcessingKernel deals with the execution of the process
        SubProcessKernel kernel = new SubProcessKernel(configuration, binaryName);

        // Run the command and work through the results
        List<String> results = kernel.exec(commands);
        for (String s : results) {
          c.output(KV.of(c.element().getKey(), s));
        }
      } catch (Exception ex) {
        LOG.error("Error processing element ", ex);
        throw ex;
      }
    }
  }
}