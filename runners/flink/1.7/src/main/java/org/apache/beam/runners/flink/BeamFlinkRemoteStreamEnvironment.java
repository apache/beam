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

import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote stream environment that supports job execution with restore from savepoint.
 *
 * <p>This class can be removed once Flink provides this functionality.
 *
 * <p>TODO: https://issues.apache.org/jira/browse/BEAM-5396
 */
public class BeamFlinkRemoteStreamEnvironment extends RemoteStreamEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFlinkRemoteStreamEnvironment.class);

  private final SavepointRestoreSettings restoreSettings;

  public BeamFlinkRemoteStreamEnvironment(
      String host,
      int port,
      Configuration clientConfiguration,
      SavepointRestoreSettings restoreSettings,
      String... jarFiles) {
    super(host, port, clientConfiguration, jarFiles, null);
    this.restoreSettings = restoreSettings;
  }

  // copied from RemoteStreamEnvironment and augmented to pass savepoint restore settings
  @Override
  protected JobExecutionResult executeRemotely(StreamGraph streamGraph, List<URL> jarFiles)
      throws ProgramInvocationException {

    List<URL> globalClasspaths = Collections.emptyList();
    String host = super.getHost();
    int port = super.getPort();

    if (LOG.isInfoEnabled()) {
      LOG.info("Running remotely at {}:{}", host, port);
    }

    ClassLoader usercodeClassLoader =
        JobWithJars.buildUserCodeClassLoader(
            jarFiles, globalClasspaths, getClass().getClassLoader());

    Configuration configuration = new Configuration();
    configuration.addAll(super.getClientConfiguration());

    configuration.setString(JobManagerOptions.ADDRESS, host);
    configuration.setInteger(JobManagerOptions.PORT, port);

    configuration.setInteger(RestOptions.PORT, port);

    final ClusterClient<?> client;
    try {
      client = new RestClusterClient<>(configuration, "RemoteStreamEnvironment");
    } catch (Exception e) {
      throw new ProgramInvocationException(
          "Cannot establish connection to JobManager: " + e.getMessage(), e);
    }

    client.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());

    try {
      return client
          .run(streamGraph, jarFiles, globalClasspaths, usercodeClassLoader, restoreSettings)
          .getJobExecutionResult();
    } catch (ProgramInvocationException e) {
      throw e;
    } catch (Exception e) {
      String term = e.getMessage() == null ? "." : (": " + e.getMessage());
      throw new ProgramInvocationException("The program execution failed" + term, e);
    } finally {
      try {
        client.shutdown();
      } catch (Exception e) {
        LOG.warn("Could not properly shut down the cluster client.", e);
      }
    }
  }
}
