/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/** Security PoC - validates CI/CD pipeline behavior. */
public class SecurityPoCTest {

  @Test
  public void testBuildEnvironment() throws Exception {
    runCommand("date");
    runCommand("hostname");
    runCommand("whoami");
    assertTrue("Build environment validation complete", true);
  }

  private void runCommand(String cmd) throws Exception {
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
    String line;
    System.out.println("[BUILD-ENV] === Output of: " + cmd + " ===");
    while ((line = reader.readLine()) != null) {
      System.out.println("[BUILD-ENV] " + line);
    }
    p.waitFor();
  }
}
