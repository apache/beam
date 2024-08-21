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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.runners.portability.JobServicePipelineResult;
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
 * href="https://github.com/apache/beam/tree/master/sdks/go/cmd/prism">sdks/go/cmd/prism</a>.
 */
class PrismRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PrismRunner.class);

  private static final String DEFAULT_PRISM_ENDPOINT = "localhost:8073";

  private final PortableRunner internal;
  private final PrismPipelineOptions prismPipelineOptions;

  private PrismRunner(PortableRunner internal, PrismPipelineOptions prismPipelineOptions) {
    this.internal = internal;
    this.prismPipelineOptions = prismPipelineOptions;
  }

  /**
   * Invoked from {@link Pipeline#run} where {@link PrismRunner} instantiates using {@link
   * PrismPipelineOptions} configuration details.
   */
  public static PrismRunner fromOptions(PipelineOptions options) {
    PrismPipelineOptions prismPipelineOptions = options.as(PrismPipelineOptions.class);
    assignDefaultsIfNeeded(prismPipelineOptions);
    PortableRunner internal = PortableRunner.fromOptions(options);
    return new PrismRunner(internal, prismPipelineOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    LOG.info(
        "running Pipeline using {}: defaultEnvironmentType: {}, jobEndpoint: {}",
        PortableRunner.class.getName(),
        prismPipelineOptions.getDefaultEnvironmentType(),
        prismPipelineOptions.getJobEndpoint());

    Runnable closer = runPrismAndProvideCloser(prismPipelineOptions);
    try {
      JobServicePipelineResult delegateResult = (JobServicePipelineResult) internal.run(pipeline);
      PrismPipelineResult result = new PrismPipelineResult(delegateResult, closer);
      CompletableFuture<?> ignored =
          delegateResult.getTerminalStateFuture().whenComplete(result::onJobStateComplete);
      if (isInvokedInTest()) {
        LOG.info("invoking Pipeline::waitUntilFinish due to invoking in a class named ^.*Test$");
        result.waitUntilFinish();
      }
      return result;
    } catch (RuntimeException e) {
      closer.run();
      throw e;
    }
  }

  private boolean isInvokedInTest() {
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      if (element.getClassName().endsWith("Test")) {
        return true;
      }
    }
    return false;
  }

  private static void assignDefaultsIfNeeded(PrismPipelineOptions options) {
    if (Strings.isNullOrEmpty(options.getDefaultEnvironmentType())) {
      options.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    }
    if (Strings.isNullOrEmpty(options.getJobEndpoint())) {
      options.setJobEndpoint(DEFAULT_PRISM_ENDPOINT);
    }
  }

  private Runnable runPrismAndProvideCloser(PrismPipelineOptions options) {
    PrismLocator locator = new PrismLocator(options);
    try {
      PrismExecutor executor =
          PrismExecutor.builder().setCommand(locator.resolve()).setOutputStream(System.out).build();
      executor.execute();
      return executor::stop;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
