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
// TODO(https://github.com/apache/beam/issues/31793): add public modifier after finalizing
//  PrismRunner. Depends on: https://github.com/apache/beam/issues/31402 and
//  https://github.com/apache/beam/issues/31792.
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

    return internal.run(pipeline);
  }

  private static void assignDefaultsIfNeeded(PrismPipelineOptions prismPipelineOptions) {
    if (Strings.isNullOrEmpty(prismPipelineOptions.getDefaultEnvironmentType())) {
      prismPipelineOptions.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    }
    if (Strings.isNullOrEmpty(prismPipelineOptions.getJobEndpoint())) {
      prismPipelineOptions.setJobEndpoint(DEFAULT_PRISM_ENDPOINT);
    }
  }
}
