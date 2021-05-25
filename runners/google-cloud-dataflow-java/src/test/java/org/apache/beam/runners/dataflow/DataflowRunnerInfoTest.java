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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link DataflowRunnerInfo}.
 *
 * <p>Note that tests for checking that the Dataflow distribution correctly loads overridden
 * properties is contained within the Dataflow distribution.
 */
public class DataflowRunnerInfoTest {

  @Test
  public void getDataflowRunnerInfo() throws Exception {
    DataflowRunnerInfo info = DataflowRunnerInfo.getDataflowRunnerInfo();

    String version = info.getLegacyEnvironmentMajorVersion();
    // Validate major version is a number
    assertTrue(
        String.format("Legacy environment major version number %s is not a number", version),
        version.matches("\\d+"));

    version = info.getFnApiEnvironmentMajorVersion();
    // Validate major version is a number
    assertTrue(
        String.format("FnAPI environment major version number %s is not a number", version),
        version.matches("\\d+"));

    // Validate container versions do not contain the property name.
    assertThat(
        "legacy container version invalid",
        info.getFnApiDevContainerVersion(),
        not(containsString("dataflow.legacy_container_version")));

    assertThat(
        "FnAPI container version invalid",
        info.getLegacyDevContainerVersion(),
        not(containsString("dataflow.fnapi_container_version")));

    // Validate container base repository does not contain the property name
    // (indicating it was not filled in).
    assertThat(
        "container repository invalid",
        info.getContainerImageBaseRepository(),
        not(containsString("dataflow.container_base_repository")));

    for (String property :
        new String[] {"java.vendor", "java.version", "os.arch", "os.name", "os.version"}) {
      assertEquals(System.getProperty(property), info.getProperties().get(property));
    }
  }
}
