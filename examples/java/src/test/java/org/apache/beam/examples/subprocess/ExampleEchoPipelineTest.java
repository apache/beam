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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.subprocess.kernel.SubProcessCommandLineArgs;
import org.apache.beam.examples.subprocess.kernel.SubProcessCommandLineArgs.Command;
import org.apache.beam.examples.subprocess.kernel.SubProcessKernel;
import org.apache.beam.examples.subprocess.utils.CallingSubProcessUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To keep {@link org.apache.beam.examples.subprocess.ExampleEchoPipeline} simple, it is not
 * factored or testable. This test file should be maintained with a copy of its code for a basic
 * smoke test.
 */
@RunWith(JUnit4.class)
public class ExampleEchoPipelineTest {

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testExampleEchoPipeline() throws Exception {

    // Create two Bash files as tests for the binary files

    Path fileA = Files.createTempFile("test-Echo", ".sh");
    Path fileB = Files.createTempFile("test-EchoAgain", ".sh");

    Path workerTempFiles = Files.createTempDirectory("test-Echoo");

    try (SeekableByteChannel channel =
        FileChannel.open(fileA, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.wrap(getTestShellEcho().getBytes(UTF_8)));
    }

    try (SeekableByteChannel channel =
        FileChannel.open(fileB, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.wrap(getTestShellEchoAgain().getBytes(UTF_8)));
    }

    // Read in the options for the pipeline
    SubProcessPipelineOptions options = PipelineOptionsFactory.as(SubProcessPipelineOptions.class);

    options.setConcurrency(2);
    options.setSourcePath(fileA.getParent().toString());
    options.setWorkerPath(workerTempFiles.toAbsolutePath().toString());

    p.getOptions().as(GcsOptions.class).setGcsUtil(buildMockGcsUtil());

    // Setup the Configuration option used with all transforms
    SubProcessConfiguration configuration = options.getSubProcessConfiguration();

    // Create some sample data to be fed to our c++ Echo library
    List<KV<String, String>> sampleData = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String str = String.valueOf(i);
      sampleData.add(KV.of(str, str));
    }

    // Define the pipeline which is two transforms echoing the inputs out to Logs
    // For this use case we will make use of two shell files instead of the binary to make
    // testing easier
    PCollection<KV<String, String>> output =
        p.apply(Create.of(sampleData))
            .apply(
                "Echo inputs round 1",
                ParDo.of(new EchoInputDoFn(configuration, fileA.getFileName().toString())))
            .apply(
                "Echo inputs round 2",
                ParDo.of(new EchoInputDoFn(configuration, fileB.getFileName().toString())));

    PAssert.that(output).containsInAnyOrder(sampleData);

    p.run();
  }

  /** Simple DoFn that echos the element, used as an example of running a C++ library. */
  @SuppressWarnings("serial")
  private static class EchoInputDoFn extends DoFn<KV<String, String>, KV<String, String>> {

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

  private static String getTestShellEcho() {
    return "#!/bin/sh\n" + "filename=$1;\n" + "echo $2 >> $filename;";
  }

  private static String getTestShellEchoAgain() {
    return "#!/bin/sh\n" + "filename=$1;\n" + "echo $2 >> $filename;";
  }

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito.when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .then(
            new Answer<SeekableByteChannel>() {

              @Override
              public SeekableByteChannel answer(InvocationOnMock invocation) throws Throwable {
                return FileChannel.open(
                    Files.createTempFile("channel-", ".tmp"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DELETE_ON_CLOSE);
              }
            });

    // Any request for expansion returns a list containing the original GcsPath
    // This is required to pass validation that occurs in TextIO during apply()
    Mockito.when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .then(
            new Answer<List<GcsPath>>() {

              @Override
              public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
                return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
              }
            });

    return mockGcsUtil;
  }
}
