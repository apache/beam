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
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ContainerLaunchUtil;
import org.apache.samza.util.SamzaUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs the beam Yarn container, using the static global job model. */
public class BeamContainerRunner implements ApplicationRunner {
  private static final Logger LOG = LoggerFactory.getLogger(BeamContainerRunner.class);

  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;

  public BeamContainerRunner(SamzaApplication app, Config config) {
    this.appDesc = ApplicationDescriptorUtil.getAppDescriptor(app, config);
  }

  @Override
  public void run(ExternalContext externalContext) {
    Thread.setDefaultUncaughtExceptionHandler(
        new SamzaUncaughtExceptionHandler(
            () -> {
              LOG.info("Exiting process now.");
              System.exit(1);
            }));

    ContainerLaunchUtil.run(
        appDesc,
        System.getenv(ShellCommandConfig.ENV_CONTAINER_ID()),
        ContainerCfgFactory.jobModel);
  }

  @Override
  public void kill() {
    // Do nothing. Yarn will kill the container.
  }

  @Override
  public ApplicationStatus status() {
    // The container is running during the life span of this object.
    return ApplicationStatus.Running;
  }

  @Override
  public void waitForFinish() {
    // Container run is synchronous
    // so calling waitForFinish() after run() should return immediately
    LOG.info("Container has stopped");
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    // Container run is synchronous
    // so calling waitForFinish() after run() should return immediately
    LOG.info("Container has stopped");
    return true;
  }
}
