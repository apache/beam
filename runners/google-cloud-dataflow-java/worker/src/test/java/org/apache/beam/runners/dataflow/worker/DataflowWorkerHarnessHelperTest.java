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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.TextFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataflowWorkerHarnessHelper}. */
@RunWith(JUnit4.class)
public class DataflowWorkerHarnessHelperTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TestRule restoreLogging = new RestoreDataflowLoggingMDC();

  private static final String JOB_ID = "2017-02-10_09_35_21-17008930546087200216";
  private static final String WORKER_ID = "worker_32132143_abcdef";

  @Test
  public void testLoggingConfiguration() throws Exception {
    DataflowWorkerHarnessOptions pipelineOptions =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    String serializedOptions = new ObjectMapper().writeValueAsString(pipelineOptions);
    File file = tmpFolder.newFile();
    Files.write(Paths.get(file.getPath()), serializedOptions.getBytes(StandardCharsets.UTF_8));
    System.setProperty("sdk_pipeline_options_file", file.getPath());

    DataflowWorkerHarnessOptions generatedOptions =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            DataflowBatchWorkerHarnessTest.class);
    // Assert that the returned options are correct.
    assertThat(generatedOptions.getJobId(), equalTo(JOB_ID));
    assertThat(generatedOptions.getWorkerId(), equalTo(WORKER_ID));

    // Assert that the global logging configuration was properly initialized.
    assertThat(DataflowWorkerLoggingMDC.getJobId(), equalTo(JOB_ID));
    assertThat(DataflowWorkerLoggingMDC.getWorkerId(), equalTo(WORKER_ID));
  }

  @Test
  public void testParseDescriptor() throws TextFormat.ParseException {
    Endpoints.ApiServiceDescriptor descriptor =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl("some_test_url").build();
    Endpoints.ApiServiceDescriptor decoded =
        DataflowWorkerHarnessHelper.parseApiServiceDescriptorFromText(descriptor.toString());

    assertThat(decoded, equalTo(descriptor));
    assertThat(decoded.getUrl(), equalTo("some_test_url"));
  }

  @Test
  public void testParseStatusApiDescriptor() throws TextFormat.ParseException {
    assertNull(DataflowWorkerHarnessHelper.getStatusDescriptor());
  }
}
