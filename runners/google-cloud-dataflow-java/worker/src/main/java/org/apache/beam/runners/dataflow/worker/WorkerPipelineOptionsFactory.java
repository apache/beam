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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory used to create {@link DataflowWorkerHarnessOptions} used during the bootstrap process
 * to initialize a Dataflow worker harness.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WorkerPipelineOptionsFactory {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerPipelineOptionsFactory.class);

  /**
   * Creates a set of Dataflow worker harness options based of a set of known system properties.
   * This is meant to only be used from the Dataflow worker harness as a method to bootstrap the
   * worker harness.
   *
   * @return A {@link DataflowWorkerHarnessOptions} object configured for the Dataflow worker
   *     harness.
   */
  public static <T extends DataflowWorkerHarnessOptions> T createFromSystemProperties(
      Class<T> harnessOptionsClass) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    T options;
    if (System.getProperties().containsKey("sdk_pipeline_options")) {
      // TODO: remove this method of getting pipeline options, once migration is complete.
      String serializedOptions = System.getProperty("sdk_pipeline_options");
      LOG.info("Worker harness starting with: {}", serializedOptions);
      options =
          objectMapper.readValue(serializedOptions, PipelineOptions.class).as(harnessOptionsClass);
    } else if (System.getProperties().containsKey("sdk_pipeline_options_file")) {
      String filePath = System.getProperty("sdk_pipeline_options_file");
      LOG.info("Loading pipeline options from " + filePath);
      String serializedOptions =
          new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
      LOG.info("Worker harness starting with: " + serializedOptions);
      options =
          objectMapper.readValue(serializedOptions, PipelineOptions.class).as(harnessOptionsClass);
    } else {
      LOG.info("Using empty PipelineOptions, as none were provided.");
      options = PipelineOptionsFactory.as(harnessOptionsClass);
    }

    // These values will not be known at job submission time and must be provided.
    if (System.getProperties().containsKey("worker_id")) {
      options.setWorkerId(System.getProperty("worker_id"));
    }
    if (System.getProperties().containsKey("job_id")) {
      options.setJobId(System.getProperty("job_id"));
    }
    if (System.getProperties().containsKey("worker_pool")) {
      options.setWorkerPool(System.getProperty("worker_pool"));
    }

    // Remove impersonate information from workers
    // More details:
    // https://cloud.google.com/dataflow/docs/reference/pipeline-options#security_and_networking
    if (options.getImpersonateServiceAccount() != null) {
      LOG.info(
          "Remove the impersonateServiceAccount pipeline option ({}) when starting the Worker harness.",
          options.getImpersonateServiceAccount());
      options.setImpersonateServiceAccount(null);
    }

    return options;
  }
}
