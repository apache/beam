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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WorkerService}. */
@RunWith(JUnit4.class)
public class WorkerServiceTest {
  @Test
  public void testStartStop() throws Exception {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    WorkerService underTest = new WorkerService(options);
    underTest.start();
    assertThat(underTest.isRunning()).isTrue();
    assertThat(underTest.getApiServiceDescriptorUrl()).matches("localhost:\\d+");
    underTest.stop();
    assertThat(underTest.isRunning()).isFalse();
  }

  @Test
  public void givenStarted_updateDefaultEnvironmentConfig() throws Exception {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    assertThat(options.getDefaultEnvironmentConfig()).isNull();
    WorkerService underTest = new WorkerService(options);
    underTest.start();
    options = underTest.updateDefaultEnvironmentConfig(options);
    assertThat(options.getDefaultEnvironmentConfig())
        .isEqualTo(underTest.getApiServiceDescriptorUrl());
    underTest.stop();
  }

  @Test
  public void givenNotStarted_updateDefaultEnvironmentConfig_throws() {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    WorkerService underTest = new WorkerService(options);
    assertThrows(
        IllegalStateException.class, () -> underTest.updateDefaultEnvironmentConfig(options));
  }

  @Test
  public void whenStateIsTerminal_thenStop() throws Exception {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    WorkerService underTest = new WorkerService(options);
    assertThat(underTest.isRunning()).isFalse();
    underTest.start();
    assertThat(underTest.isRunning()).isTrue();

    underTest.onStateChanged(PipelineResult.State.RUNNING);
    assertThat(underTest.isRunning()).isTrue();

    underTest.onStateChanged(PipelineResult.State.RUNNING);
    assertThat(underTest.isRunning()).isTrue();

    underTest.onStateChanged(PipelineResult.State.CANCELLED);
    assertThat(underTest.isRunning()).isFalse();
  }
}
