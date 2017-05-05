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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

/**
 * A {@link FlinkStreamingPipelineJob} that runs on a Flink cluster.
 */
class FlinkRemoteStreamingPipelineJob extends FlinkStreamingPipelineJob {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkRemoteStreamingPipelineJob.class);

  private final JobID jobId;
  private final FiniteDuration clientTimeout = FiniteDuration.apply(10, "seconds");
  private final Configuration configuration;

  public FlinkRemoteStreamingPipelineJob(
      FlinkPipelineOptions pipelineOptions,
      RemoteStreamEnvironment flinkEnv) throws Exception {

    // transform the streaming program into a JobGraph
    StreamGraph streamGraph = flinkEnv.getStreamGraph();
    streamGraph.setJobName(pipelineOptions.getJobName());

    List<String> stagingFiles = pipelineOptions.getFilesToStage();

    configuration = new Configuration();
    configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, flinkEnv.getHost());
    configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, flinkEnv.getPort());

    ClusterClient client;
    try {
      client = new StandaloneClusterClient(configuration);
      client.setPrintStatusDuringExecution(flinkEnv.getConfig().isSysoutLoggingEnabled());
    } catch (Exception e) {
      throw new ProgramInvocationException(
          "Cannot establish connection to JobManager: " + e.getMessage(), e);
    }

    List<URL> stagingUrls = new LinkedList<>();
    if (stagingFiles != null) {
      for (String jarFile : stagingFiles) {
        try {
          stagingUrls.add(new File(jarFile).getAbsoluteFile().toURI().toURL());
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException("Staging file path invalid", e);
        }
      }
    }

    ClassLoader usercodeClassLoader =
        JobWithJars.buildUserCodeClassLoader(
            stagingUrls, Collections.<URL>emptyList(), getClass().getClassLoader());

    JobGraph jobGraph = streamGraph.getJobGraph();

    for (URL jar : stagingUrls) {
      try {
        jobGraph.addJar(new Path(jar.toURI()));
      } catch (URISyntaxException e) {
        throw new RuntimeException("URL is invalid. This should not happen.", e);
      }
    }

    jobGraph.setClasspaths(Collections.<URL>emptyList());

    try {
      jobId = client.runDetached(jobGraph, usercodeClassLoader).getJobID();
      LOG.info("Submitted job with JobId {}", jobId);
    } catch (Exception e) {
      String term = e.getMessage() == null ? "." : (": " + e.getMessage());
      throw new ProgramInvocationException("The program execution failed" + term, e);
    } finally {
      client.shutdown();
    }
  }

  @Override
  protected Configuration getConfiguration() {
    return configuration;
  }

  @Override
  protected FiniteDuration getClientTimeout() {
    return clientTimeout;
  }

  @Override
  public JobID getJobId() {
    return jobId;
  }
}
