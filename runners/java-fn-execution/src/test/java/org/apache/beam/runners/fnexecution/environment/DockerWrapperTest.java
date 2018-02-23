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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.runners.fnexecution.environment.testing.NeedsDocker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DockerWrapper}. */
@Category(NeedsDocker.class)
@RunWith(JUnit4.class)
public class DockerWrapperTest {

  @Test
  public void helloWorld() throws Exception {
    DockerWrapper docker = getWrapper();
    String container = docker.runImage("hello-world", Collections.emptyList());
    System.out.printf("Started container: %s%n", container);
  }

  @Test
  public void killContainer() throws Exception {
    DockerWrapper docker = getWrapper();
    String container = docker.runImage("debian", Arrays.asList("/bin/bash", "-c", "sleep 60"));
    docker.killContainer(container);
  }

  private static DockerWrapper getWrapper() {
    return DockerWrapper.forCommand(
        "docker", Duration.ofMillis(10000));
  }

}
