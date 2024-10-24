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
package org.apache.beam.runners.flink;

import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server for the Flink runner. */
public class FlinkJobServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriver.class);

  /** Flink runner-specific Configuration for the jobServer. */
  public static class FlinkServerConfiguration extends ServerConfiguration {
    @Option(
        name = "--flink-master",
        aliases = {"--flink-master-url"},
        usage =
            "Flink master address (host:port) to submit the job against. Use Use \"[local]\" to start a local "
                + "cluster for the execution. Use \"[auto]\" if you plan to either execute locally or submit through "
                + "Flink\'s CLI.")
    private String flinkMaster = FlinkPipelineOptions.AUTO;

    String getFlinkMaster() {
      return this.flinkMaster;
    }

    @Option(
        name = "--flink-conf-dir",
        usage =
            "Directory containing Flink YAML configuration files. "
                + "These properties will be set to all jobs submitted to Flink and take precedence "
                + "over configurations in FLINK_CONF_DIR.")
    private @Nullable String flinkConfDir = null;

    @Nullable
    String getFlinkConfDir() {
      return flinkConfDir;
    }
  }

  public static void main(String[] args) throws Exception {
    // TODO: Expose the fileSystem related options.
    PipelineOptions options = PipelineOptionsFactory.create();
    // Limiting gcs upload buffer to reduce memory usage while doing parallel artifact uploads.
    options.as(GcsOptions.class).setGcsUploadBufferSizeBytes(1024 * 1024);
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", FlinkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static FlinkServerConfiguration parseArgs(String[] args) {
    FlinkServerConfiguration configuration = new FlinkServerConfiguration();
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

  // this method is used via reflection in TestPortableRunner
  public static FlinkJobServerDriver fromParams(String[] args) {
    return fromConfig(parseArgs(args));
  }

  public static FlinkJobServerDriver fromConfig(FlinkServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration),
        () -> FlinkJobInvoker.create(configuration));
  }

  public static FlinkJobServerDriver fromConfig(
      FlinkServerConfiguration configuration, JobInvokerFactory jobInvokerFactory) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration),
        jobInvokerFactory);
  }

  private static FlinkJobServerDriver create(
      FlinkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    return new FlinkJobServerDriver(
        configuration, jobServerFactory, artifactServerFactory, jobInvokerFactory);
  }

  private FlinkJobServerDriver(
      FlinkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    super(configuration, jobServerFactory, artifactServerFactory, jobInvokerFactory);
  }
}
