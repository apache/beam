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
package org.apache.beam.runners.samza.container;

import java.time.Duration;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.LocalContainerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs the beam Yarn container, using the static global job model. */
public class BeamContainerRunner extends LocalContainerRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerCfgFactory.class);

  public BeamContainerRunner(Config config) {
    super(ContainerCfgFactory.jobModel, System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()));
  }

  @Override
  public ApplicationStatus status(StreamApplication app) {
    return ApplicationStatus.Running;
  }

  @Override
  public void waitForFinish() {
    LOG.info("Container has stopped");
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    LOG.info("Container has stopped");
    return true;
  }
}
