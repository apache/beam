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
import static org.apache.beam.runners.prism.PrismRunnerTest.getLocalPrismBuildOrIgnoreTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrismExecutor executor = underTest(baos).build();
    executor.execute();
    sleep(3000L);
    executor.stop();
    String got = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(got).contains("INFO Serving JobManagement endpoint=localhost:8073");
  }

  private PrismExecutor.Builder underTest(OutputStream outputStream) {
    return PrismExecutor.builder()
        .setCommand(getLocalPrismBuildOrIgnoreTest())
        .setOutputStream(outputStream);
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
  }
}
