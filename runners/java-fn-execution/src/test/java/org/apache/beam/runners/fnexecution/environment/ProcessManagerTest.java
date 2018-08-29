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

package org.apache.beam.runners.fnexecution.environment;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProcessManagerTest {

  @Test
  public void testRunSimpleCommand() throws IOException {
    ProcessManager processManager = ProcessManager.getInstance();
    processManager.startProcess("1", "ls", Collections.emptyList());
    processManager.stopProcess("1");
    processManager.startProcess("2", "ls", Collections.singletonList("-l"));
    processManager.stopProcess("2");
    processManager.startProcess("1", "ls", Arrays.asList("-l", "-a"));
    processManager.stopProcess("1");
  }

  @Test
  public void testRunInvalidExecutable() throws IOException {
    ProcessManager processManager = ProcessManager.getInstance();
    try {
      processManager.startProcess("1", "asfasfls", Collections.emptyList());
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("Cannot run program \"asfasfls\""));
    }
  }

  @Test
  public void testDuplicateId() throws IOException {
    ProcessManager processManager = ProcessManager.getInstance();
    processManager.startProcess("1", "ls", Collections.emptyList());
    try {
      processManager.startProcess("1", "ls", Collections.emptyList());
      fail();
    } catch (IllegalStateException e) {
      // this is what we want
    } finally {
      processManager.stopProcess("1");
    }
  }

  @Test
  public void testLivenessCheck() throws IOException {
    ProcessManager processManager = ProcessManager.getInstance();
    ProcessManager.RunningProcess process =
        processManager.startProcess("1", "sleep", Collections.singletonList("1000"));
    process.isAliveOrThrow();
    processManager.stopProcess("1");
    try {
      process.isAliveOrThrow();
      fail();
    } catch (IllegalStateException e) {
      // this is what we want
    }
  }

  @Test
  public void testEnvironmentVariables() throws IOException {
    ProcessManager processManager = ProcessManager.getInstance();
    ProcessManager.RunningProcess process =
        processManager.startProcess(
            "1",
            "/bin/bash",
            Arrays.asList("-c", "'echo $WAIT_FOR > /tmp/bla'"),
            Collections.singletonMap("WAIT_FOR", "1000"));
    process.isAliveOrThrow();
    processManager.stopProcess("1");
  }
}
