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

import javax.annotation.Nullable;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
    @Option(name = "--flink-master-url", usage = "Flink master url to submit job.")
    private String flinkMasterUrl = "[auto]";

    String getFlinkMasterUrl() {
      return this.flinkMasterUrl;
    }

    @Option(
        name = "--flink-conf-dir",
        usage =
            "Directory containing Flink YAML configuration files. "
                + "These properties will be set to all jobs submitted to Flink and take precedence "
                + "over configurations in FLINK_CONF_DIR.")
    private String flinkConfDir = null;

    @Nullable
    String getFlinkConfDir() {
      return flinkConfDir;
    }
  }

  public static void main(String[] args) throws Exception {
    // TODO: Expose the fileSystem related options.
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", FlinkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static FlinkJobServerDriver fromParams(String[] args) {
    FlinkServerConfiguration configuration = new FlinkServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }

    return fromConfig(configuration);
  }

  public static FlinkJobServerDriver fromConfig(FlinkServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration));
  }

  public static FlinkJobServerDriver create(
      FlinkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new FlinkJobServerDriver(configuration, jobServerFactory, artifactServerFactory);
  }

  private FlinkJobServerDriver(
      FlinkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    super(configuration, jobServerFactory, artifactServerFactory);
  }

  @Override
  protected JobInvoker createJobInvoker() {
    return FlinkJobInvoker.create((FlinkServerConfiguration) configuration);
  }
}
