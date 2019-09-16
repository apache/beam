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
package org.apache.beam.runners.reference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.construction.InMemoryArtifactStagerService;
import org.apache.beam.runners.reference.testing.TestJobService;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PortableRunner}. */
@RunWith(JUnit4.class)
public class PortableRunnerTest implements Serializable {

  private static final String ENDPOINT_URL = "foo:3000";
  private static final ApiServiceDescriptor ENDPOINT_DESCRIPTOR =
      ApiServiceDescriptor.newBuilder().setUrl(ENDPOINT_URL).build();

  private final PipelineOptions options = createPipelineOptions();

  @Rule public transient TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void stagesAndRunsJob() throws Exception {
    try (CloseableResource<Server> server = createJobServer(JobState.Enum.DONE)) {
      PortableRunner runner =
          PortableRunner.create(options, InProcessManagedChannelFactory.create());
      State state = runner.run(p).waitUntilFinish();
      assertThat(state, is(State.DONE));
    }
  }

  private static CloseableResource<Server> createJobServer(JobState.Enum jobState)
      throws IOException {
    CloseableResource<Server> server =
        CloseableResource.of(
            InProcessServerBuilder.forName(ENDPOINT_URL)
                .addService(new TestJobService(ENDPOINT_DESCRIPTOR, "prepId", "jobId", jobState))
                .addService(new InMemoryArtifactStagerService())
                .build(),
            Server::shutdown);
    server.get().start();
    return server;
  }

  private static PipelineOptions createPipelineOptions() {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    options.setJobEndpoint(ENDPOINT_URL);
    options.setRunner(PortableRunner.class);
    return options;
  }
}
