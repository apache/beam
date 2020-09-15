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
package org.apache.beam.runners.spark;

import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server for the Spark runner. */
public class SparkJobServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJobServerDriver.class);

  /** Spark runner-specific Configuration for the jobServer. */
  public static class SparkServerConfiguration extends ServerConfiguration {
    @Option(
        name = "--spark-master-url",
        usage = "Spark master url to submit job (e.g. spark://host:port, local[4])")
    private String sparkMasterUrl = SparkPipelineOptions.DEFAULT_MASTER_URL;

    String getSparkMasterUrl() {
      return this.sparkMasterUrl;
    }
  }

  public static void main(String[] args) {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", SparkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  private static SparkJobServerDriver fromParams(String[] args) {
    SparkServerConfiguration configuration = new SparkServerConfiguration();
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

  private static SparkJobServerDriver fromConfig(SparkServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration));
  }

  private static SparkJobServerDriver create(
      SparkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new SparkJobServerDriver(configuration, jobServerFactory, artifactServerFactory);
  }

  private SparkJobServerDriver(
      SparkServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    super(
        configuration,
        jobServerFactory,
        artifactServerFactory,
        () -> SparkJobInvoker.create(configuration));
  }
}
