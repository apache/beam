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
import static org.apache.beam.runners.prism.PrismPipelineOptions.JOB_PORT_FLAG_NAME;
import static org.apache.beam.runners.prism.PrismRunnerTest.getLocalPrismBuildOrIgnoreTest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismExecutor}. */
@RunWith(JUnit4.class)
public class PrismExecutorTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public TestName testName = new TestName();

  @Test
  public void executeThenStop() throws IOException {
    PrismExecutor executor = underTest().build();
    executor.execute();
    sleep(3000L);
    executor.stop();
  }

  @Test
  public void executeWithStreamRedirectThenStop() throws IOException {
    PrismExecutor executor = underTest().build();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    executor.execute(outputStream);
    sleep(3000L);
    executor.stop();
    String output = normalizeConsoleOutput(outputStream.toString(StandardCharsets.UTF_8.name()));
    assertThat(output).contains("INFO Serving JobManagement endpoint=localhost:8073");
  }

  @Test
  public void executeWithFileOutputThenStop() throws IOException {
    PrismExecutor executor = underTest().build();
    File log = temporaryFolder.newFile(testName.getMethodName());
    executor.execute(log);
    sleep(3000L);
    executor.stop();
    try (Stream<String> stream = Files.lines(log.toPath(), StandardCharsets.UTF_8)) {
      String output = normalizeConsoleOutput(stream.collect(Collectors.joining("\n")));
      assertThat(output).contains("INFO Serving JobManagement endpoint=localhost:8073");
    }
  }

  @Test
  public void executeWithCustomArgumentsThenStop() throws IOException {
    PrismExecutor executor =
        underTest()
            .setArguments(Collections.singletonList("-" + JOB_PORT_FLAG_NAME + "=5555"))
            .build();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    executor.execute(outputStream);
    sleep(3000L);
    executor.stop();
    String output = normalizeConsoleOutput(outputStream.toString(StandardCharsets.UTF_8.name()));
    assertThat(output).contains("INFO Serving JobManagement endpoint=localhost:5555");
  }

  @Test
  public void executeWithPortFinderThenStop() throws IOException {}

  private PrismExecutor.Builder underTest() {
    return PrismExecutor.builder().setCommand(getLocalPrismBuildOrIgnoreTest());
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
  }

  private static String normalizeConsoleOutput(String output) {
    return output
        .replaceAll("\u001B\\[[;\\d]*m", "") // remove ASCII color codes
        .replaceAll("\\s\\* ", " ") // clean up bullet points
        .replaceAll("\\s+", " ") // clean up multiple spaces
        .replaceAll("endpoint: ", "endpoint="); // for ease of assertion
  }
}
