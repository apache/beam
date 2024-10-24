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
package org.apache.beam.runners.prism;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} executed on Prism. Downloads, prepares, and executes the Prism service
 * on behalf of the developer when {@link PipelineRunner#run}ning the pipeline. If users want to
 * submit to an already running Prism service, use the {@link PortableRunner} with the {@link
 * PortablePipelineOptions#getJobEndpoint()} option instead. Prism is a {@link
 * org.apache.beam.runners.portability.PortableRunner} maintained at <a
 * href="https://github.com/apache/beam/tree/master/sdks/go/cmd/prism">sdks/go/cmd/prism</a>. For
 * testing, use {@link TestPrismRunner}.
 */
public class PrismRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PrismRunner.class);

  private final PrismPipelineOptions prismPipelineOptions;

  protected PrismRunner(PrismPipelineOptions prismPipelineOptions) {
    this.prismPipelineOptions = prismPipelineOptions;
  }

  PrismPipelineOptions getPrismPipelineOptions() {
    return prismPipelineOptions;
  }

  /**
   * Invoked from {@link Pipeline#run} where {@link PrismRunner} instantiates using {@link
   * PrismPipelineOptions} configuration details.
   */
  public static PrismRunner fromOptions(PipelineOptions options) {
    PrismPipelineOptions prismPipelineOptions = options.as(PrismPipelineOptions.class);
    validate(prismPipelineOptions);
    assignDefaultsIfNeeded(prismPipelineOptions);
    return new PrismRunner(prismPipelineOptions);
  }

  private static void validate(PrismPipelineOptions options) {
    checkArgument(
        Strings.isNullOrEmpty(options.getJobEndpoint()),
        "when specifying --jobEndpoint, use --runner=PortableRunner instead");
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    LOG.info(
        "running Pipeline using {}: defaultEnvironmentType: {}, jobEndpoint: {}",
        PortableRunner.class.getName(),
        prismPipelineOptions.getDefaultEnvironmentType(),
        prismPipelineOptions.getJobEndpoint());

    try {
      PrismExecutor executor = startPrism();
      PortableRunner delegate = PortableRunner.fromOptions(prismPipelineOptions);
      return new PrismPipelineResult(delegate.run(pipeline), executor::stop);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  PrismExecutor startPrism() throws IOException {
    PrismLocator locator = new PrismLocator(prismPipelineOptions);
    int port = findAvailablePort();
    String portFlag = String.format(PrismExecutor.JOB_PORT_FLAG_TEMPLATE, port);
    String serveHttpFlag =
        String.format(
            PrismExecutor.SERVE_HTTP_FLAG_TEMPLATE, prismPipelineOptions.getEnableWebUI());
    String idleShutdownTimeoutFlag =
        String.format(
            PrismExecutor.IDLE_SHUTDOWN_TIMEOUT, prismPipelineOptions.getIdleShutdownTimeout());
    String logLevelFlag =
        String.format(
            PrismExecutor.LOG_LEVEL_FLAG_TEMPLATE, prismPipelineOptions.getPrismLogLevel());
    String endpoint = "localhost:" + port;
    prismPipelineOptions.setJobEndpoint(endpoint);
    String command = locator.resolve();
    PrismExecutor executor =
        PrismExecutor.builder()
            .setCommand(command)
            .setArguments(Arrays.asList(portFlag, serveHttpFlag, idleShutdownTimeoutFlag, logLevelFlag))
            .build();
    executor.execute();
    checkState(executor.isAlive());
    return executor;
  }

  private static void assignDefaultsIfNeeded(PrismPipelineOptions prismPipelineOptions) {
    if (Strings.isNullOrEmpty(prismPipelineOptions.getDefaultEnvironmentType())) {
      prismPipelineOptions.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    }
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
