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
import org.apache.samza.clustermanager.JobCoordinatorLaunchUtil;
import org.apache.samza.config.Config;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;

/** Runs on Yarn AM, execute planning and launches JobCoordinator. */
public class BeamJobCoordinatorRunner implements ApplicationRunner {

  @SuppressWarnings("rawtypes")
  private final SamzaApplication<? extends ApplicationDescriptor> app;

  private final Config config;

  /**
   * Constructors a {@link BeamJobCoordinatorRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   */
  @SuppressWarnings("rawtypes")
  public BeamJobCoordinatorRunner(
      SamzaApplication<? extends ApplicationDescriptor> app, Config config) {
    this.app = app;
    this.config = config;
  }

  @Override
  public void run(ExternalContext externalContext) {
    JobCoordinatorLaunchUtil.run(app, config);
  }

  @Override
  public void kill() {
    throw new UnsupportedOperationException(
        "BeamJobCoordinatorRunner#kill should never be invoked.");
  }

  @Override
  public ApplicationStatus status() {
    throw new UnsupportedOperationException(
        "BeamJobCoordinatorRunner#status should never be invoked.");
  }

  @Override
  public void waitForFinish() {
    throw new UnsupportedOperationException(
        "BeamJobCoordinatorRunner#waitForFinish should never be invoked.");
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    throw new UnsupportedOperationException(
        "BeamJobCoordinatorRunner#waitForFinish should never be invoked.");
  }
}
