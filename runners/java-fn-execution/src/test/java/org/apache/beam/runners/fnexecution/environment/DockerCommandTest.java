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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.fnexecution.environment.testing.NeedsDocker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DockerCommand}. */
@Category(NeedsDocker.class)
@RunWith(JUnit4.class)
public class DockerCommandTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void helloWorld() throws Exception {
    DockerCommand docker = DockerCommand.getDefault();
    String container = docker.runImage("hello-world", ImmutableList.of(), ImmutableList.of());
    System.out.printf("Started container: %s%n", container);
  }

  @Test
  public void killContainer() throws Exception {
    DockerCommand docker = DockerCommand.getDefault();
    String container =
        docker.runImage(
            "debian", ImmutableList.of(), ImmutableList.of("/bin/bash", "-c", "sleep 60"));
    Stopwatch stopwatch = Stopwatch.createStarted();
    assertThat("Container should be running.", docker.isContainerRunning(container), is(true));
    docker.killContainer(container);
    long elapsedSec = stopwatch.elapsed(TimeUnit.SECONDS);
    assertThat(
        "Container termination should complete before image self-exits",
        elapsedSec,
        is(lessThan(60L)));
    assertThat("Container should be terminated.", docker.isContainerRunning(container), is(false));
  }

  @Test
  public void capturesErrorOutput() throws Exception {
    DockerCommand docker = DockerCommand.getDefault();
    thrown.expect(instanceOf(IOException.class));
    thrown.expectMessage(containsString("Error response from daemon"));
    String badImageName = "this-image-should-hopefully-never-exist";
    String container = docker.runImage(badImageName, ImmutableList.of(), ImmutableList.of());
    // We should never reach this line, but clean up in case we do.
    docker.killContainer(container);
    Assert.fail(String.format("Container creation for %s should have failed", badImageName));
  }
}
