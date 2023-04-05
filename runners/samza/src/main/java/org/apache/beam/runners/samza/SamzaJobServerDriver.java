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
package org.apache.beam.runners.samza;

import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server for the Samza runner. */
public class SamzaJobServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobServerDriver.class);

  /** Samza runner-specific Configuration for the jobServer. */
  public static class SamzaServerConfiguration extends ServerConfiguration {}

  public static void main(String[] args) {
    // TODO: Expose the fileSystem related options.
    PipelineOptions options = PipelineOptionsFactory.create();
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    fromParams(args).run();
  }

  private static SamzaJobServerDriver fromParams(String[] args) {
    return fromConfig(parseArgs(args));
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.printf("Usage: java %s arguments...%n", SamzaJobServerDriver.class.getSimpleName());
    parser.printUsage(System.err);
    System.err.println();
  }

  private static SamzaJobServerDriver fromConfig(SamzaServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration));
  }

  public static SamzaServerConfiguration parseArgs(String[] args) {
    SamzaServerConfiguration configuration = new SamzaServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }

  private static SamzaJobServerDriver create(
      SamzaServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new SamzaJobServerDriver(configuration, jobServerFactory, artifactServerFactory);
  }

  private SamzaJobServerDriver(
      SamzaServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    this(
        configuration,
        jobServerFactory,
        artifactServerFactory,
        () -> SamzaJobInvoker.create(configuration));
  }

  protected SamzaJobServerDriver(
      ServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    super(configuration, jobServerFactory, artifactServerFactory, jobInvokerFactory);
  }
}
