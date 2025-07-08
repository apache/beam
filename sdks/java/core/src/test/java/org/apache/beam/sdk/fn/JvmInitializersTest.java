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
package org.apache.beam.sdk.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JvmInitializers}. */
@RunWith(JUnit4.class)
public final class JvmInitializersTest {

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(JvmInitializers.class);

  private static Boolean onStartupRan;
  private static Boolean beforeProcessingRan;
  private static PipelineOptions receivedOptions;

  /** Test initializer implementation. Methods simply produce observable side effects. */
  @AutoService(JvmInitializer.class)
  public static class TestInitializer implements JvmInitializer {
    @Override
    public void onStartup() {
      onStartupRan = true;
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {
      beforeProcessingRan = true;
      receivedOptions = options;
    }
  }

  @Before
  public void setUp() {
    onStartupRan = false;
    beforeProcessingRan = false;
    receivedOptions = null;
  }

  @Test
  public void runOnStartup_runsInitializers() throws IOException, UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PrintStream out = System.out;
    try (PrintStream ps = new PrintStream(baos, false, "UTF8")) {
      System.setOut(ps);
      JvmInitializers.runOnStartup();
    } finally {
      System.setOut(out);
    }

    assertTrue(onStartupRan);
    assertThat(
        () ->
            new Scanner(new ByteArrayInputStream(baos.toByteArray()), "UTF8")
                .useDelimiter(System.lineSeparator()),
        hasItem(containsString("Running JvmInitializer#onStartup")));
  }

  @Test
  public void runBeforeProcessing_runsInitializersWithOptions() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();

    JvmInitializers.runBeforeProcessing(options);

    assertTrue(beforeProcessingRan);
    assertEquals(options, receivedOptions);
    expectedLogs.verifyInfo("Running JvmInitializer#beforeProcessing");
  }
}
