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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WorkerPipelineOptionsFactory}. */
@RunWith(JUnit4.class)
public class WorkerPipelineOptionsFactoryTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void testCreationFromSystemProperties() throws Exception {
    System.getProperties()
        .putAll(
            ImmutableMap.<String, String>builder()
                .put("worker_id", "test_worker_id")
                .put("job_id", "test_job_id")
                // Set a non-default value for testing
                .put("sdk_pipeline_options", "{\"options\":{\"numWorkers\":999}}")
                .build());

    @SuppressWarnings("deprecation") // testing deprecated functionality
    DataflowWorkerHarnessOptions options =
        WorkerPipelineOptionsFactory.createFromSystemProperties(DataflowWorkerHarnessOptions.class);
    assertEquals("test_worker_id", options.getWorkerId());
    assertEquals("test_job_id", options.getJobId());
    assertEquals(999, options.getNumWorkers());
  }

  @Test
  public void testCreationWithPipelineOptionsFile() throws Exception {
    File file = tmpFolder.newFile();
    String jsonOptions = "{\"options\":{\"numWorkers\":1000}}";
    Files.write(Paths.get(file.getPath()), jsonOptions.getBytes(StandardCharsets.UTF_8));
    System.getProperties()
        .putAll(
            ImmutableMap.<String, String>builder()
                .put("worker_id", "test_worker_id_2")
                .put("job_id", "test_job_id_2")
                // Set a non-default value for testing
                .put("sdk_pipeline_options_file", file.getPath())
                .build());

    @SuppressWarnings("deprecation") // testing deprecated functionality
    DataflowWorkerHarnessOptions options =
        WorkerPipelineOptionsFactory.createFromSystemProperties(DataflowWorkerHarnessOptions.class);
    assertEquals("test_worker_id_2", options.getWorkerId());
    assertEquals("test_job_id_2", options.getJobId());
    assertEquals(1000, options.getNumWorkers());
  }
}
