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
package org.apache.beam.runners.kafka.streams;

import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver that starts a Beam job server for the Kafka Streams portable runner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KafkaStreamsJobServerDriver extends JobServerDriver {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsJobServerDriver.class);

  /** Runner-specific configuration for the job server process. */
  public static class KafkaStreamsServerConfiguration extends ServerConfiguration {}

  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(GcsOptions.class).setGcsUploadBufferSizeBytes(1024 * 1024);
    FileSystems.setDefaultPipelineOptions(options);
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format(
            "Usage: java %s arguments...", KafkaStreamsJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static KafkaStreamsServerConfiguration parseArgs(String[] args) {
    KafkaStreamsServerConfiguration configuration = new KafkaStreamsServerConfiguration();
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

  /** Used by tests and tooling to construct a driver from command-line parameters. */
  public static KafkaStreamsJobServerDriver fromParams(String[] args) {
    return fromConfig(parseArgs(args));
  }

  public static KafkaStreamsJobServerDriver fromConfig(
      KafkaStreamsServerConfiguration configuration) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration),
        () -> KafkaStreamsJobInvoker.create(configuration));
  }

  public static KafkaStreamsJobServerDriver fromConfig(
      KafkaStreamsServerConfiguration configuration, JobInvokerFactory jobInvokerFactory) {
    return create(
        configuration,
        createJobServerFactory(configuration),
        createArtifactServerFactory(configuration),
        jobInvokerFactory);
  }

  private static KafkaStreamsJobServerDriver create(
      KafkaStreamsServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    return new KafkaStreamsJobServerDriver(
        configuration, jobServerFactory, artifactServerFactory, jobInvokerFactory);
  }

  private KafkaStreamsJobServerDriver(
      KafkaStreamsServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    super(configuration, jobServerFactory, artifactServerFactory, jobInvokerFactory);
  }
}
