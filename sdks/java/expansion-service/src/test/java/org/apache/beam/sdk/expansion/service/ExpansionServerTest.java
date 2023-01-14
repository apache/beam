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
package org.apache.beam.sdk.expansion.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

import java.util.Arrays;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.junit.Test;

/** Tests for {@link ExpansionServer}. */
public class ExpansionServerTest {

  @Test
  public void testStartupOnFreePort() throws Exception {
    try (ExpansionServer expansionServer =
        ExpansionServer.create(new ExpansionService(), "localhost", 0)) {
      assertThat(expansionServer.getHost(), is("localhost"));
      assertThat(expansionServer.getPort(), greaterThan(0));
    }
  }

  @Test
  public void testHostPortAvailableAfterClose() throws Exception {
    ExpansionServer expansionServer;
    try (ExpansionServer expServer =
        ExpansionServer.create(new ExpansionService(), "localhost", 0)) {
      expansionServer = expServer;
    }
    assertThat(expansionServer.getHost(), is("localhost"));
    assertThat(expansionServer.getPort(), greaterThan(0));
  }

  @Test
  public void testPassingPipelineArguments() {
    String[] args = {
      "--defaultEnvironmentType=PROCESS",
      "--defaultEnvironmentConfig={\"command\": \"/opt/apache/beam/boot\"}"
    };
    ExpansionService service = new ExpansionService(args);
    assertThat(
        service
            .createPipeline()
            .getOptions()
            .as(PortablePipelineOptions.class)
            .getDefaultEnvironmentType(),
        equalTo("PROCESS"));
  }

  @Test
  public void testNonEmptyFilesToStage() {
    String[] args = {"--filesToStage=nonExistent1.jar,nonExistent2.jar"};
    ExpansionService service = new ExpansionService(args);
    assertThat(
        service.createPipeline().getOptions().as(PortablePipelineOptions.class).getFilesToStage(),
        equalTo(Arrays.asList("nonExistent1.jar", "nonExistent2.jar")));
  }

  @Test
  public void testEmptyFilesToStageIsOK() {
    String[] args = {"--filesToStage="};
    ExpansionService service = new ExpansionService(args);
    assertThat(
        service.createPipeline().getOptions().as(PortablePipelineOptions.class).getFilesToStage(),
        equalTo(Arrays.asList("")));
  }
}
