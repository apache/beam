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
package org.apache.beam.sdk;

import static org.apache.beam.sdk.testing.ExpectedLogs.verifyLogged;
import static org.apache.beam.sdk.testing.ExpectedLogs.verifyNotLogged;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;

import java.security.Security;
import java.util.logging.Level;
import java.util.logging.LogManager;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel;
import org.apache.beam.sdk.options.SdkHarnessOptions.SdkHarnessLogLevelOverrides;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.ExpectedLogs.LogSaver;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSdkHarnessEnvironment;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that validate the SDK harness is configured correctly for a runner. */
@RunWith(JUnit4.class)
public class SdkHarnessEnvironmentTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  /**
   * {@link DoFn} used to validate that Jamm was setup as a java agent to get accurate measuring.
   */
  private static class JammDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      MemoryMeter memoryMeter = MemoryMeter.builder().withGuessing(Guess.INSTRUMENTATION).build();
      assertThat(memoryMeter.measureDeep(c.element()), greaterThan(0L));
      c.output("measured");
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSdkHarnessEnvironment.class})
  public void testJammAgentAvailable() throws Exception {
    PCollection<String> input = p.apply(Create.of("jamm").withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(ParDo.of(new JammDoFn()));

    PAssert.that(output).containsInAnyOrder("measured");
    p.run().waitUntilFinish();
  }

  /** {@link DoFn} used to validate that TLS was enabled as part of java security properties. */
  private static class TLSDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] disabledAlgorithms =
          Security.getProperty("jdk.tls.disabledAlgorithms").trim().split("\\s*,\\s*");
      String[] legacyAlgorithms =
          Security.getProperty("jdk.tls.legacyAlgorithms").trim().split("\\s*,\\s*");
      assertThat(disabledAlgorithms, not(hasItemInArray("TLSv1")));
      assertThat(disabledAlgorithms, not(hasItemInArray("TLSv1.1")));
      assertThat(legacyAlgorithms, hasItemInArray("TLSv1"));
      assertThat(legacyAlgorithms, hasItemInArray("TLSv1.1"));

      // getDefaultSSLParameters() shows all protocols that JSSE implements that are allowed.
      // getSupportedSSLParameters() shows all protocols that JSSE implements including those that
      // are disabled.
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, null, null);
      assertNotNull(context);
      String[] defaultProtocols = context.getDefaultSSLParameters().getProtocols();
      assertThat(defaultProtocols, hasItemInArray("TLSv1"));
      assertThat(defaultProtocols, hasItemInArray("TLSv1.1"));

      c.output("TLSv1-TLSv1.1 enabled");
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSdkHarnessEnvironment.class})
  public void testTlsAvailable() throws Exception {
    PCollection<String> input = p.apply(Create.of("TLS").withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(ParDo.of(new TLSDoFn()));

    PAssert.that(output).containsInAnyOrder("TLSv1-TLSv1.1 enabled");

    p.run().waitUntilFinish();
  }

  private static class LoggingDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> output) {
      LogSaver logSaver = new LogSaver();
      LogManager.getLogManager().getLogger("").addHandler(logSaver);

      try {
        Exception fooException = new RuntimeException("a.Foo-RuntimeException");
        // Test the different log levels for various named loggers.
        final org.slf4j.Logger fooLogger = org.slf4j.LoggerFactory.getLogger("a.Foo");
        fooLogger.trace("a.Foo-Trace");
        fooLogger.debug("a.Foo-Debug");
        fooLogger.info("a.Foo-Info");
        fooLogger.warn("a.Foo-Warn");
        fooLogger.error("a.Foo-Error", fooException);

        Exception barException = new RuntimeException("a.b.Bar-RuntimeException");
        final org.slf4j.Logger barLogger = org.slf4j.LoggerFactory.getLogger("a.b.Bar");
        barLogger.trace("a.b.Bar-Trace");
        barLogger.debug("a.b.Bar-Debug");
        barLogger.info("a.b.Bar-Info");
        barLogger.warn("a.b.Bar-Warn");
        barLogger.error("a.b.Bar-Error", barException);

        // Test the different types of loggers (e.g. slf4j, jcl, jul, log4j, log4jc)
        final org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger("logger.slf4j");
        slf4jLogger.info("SLF4J log messages work");
        final org.apache.commons.logging.Log jclLogger =
            org.apache.commons.logging.LogFactory.getLog("logger.jcl");
        jclLogger.info("JCL log messages work");
        final java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger("logger.jul");
        julLogger.info("JUL log messages work");
        final org.apache.log4j.Logger log4jLogger =
            org.apache.log4j.Logger.getLogger("logger.log4j");
        log4jLogger.info("Log4j log messages work");
        final org.apache.logging.log4j.Logger log4j2Logger =
            org.apache.logging.log4j.LogManager.getLogger("logger.log4j2");
        log4j2Logger.info("Log4j2 log messages work");

        verifyNotLogged(ExpectedLogs.matcher(Level.FINEST, "a.Foo-Trace"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.FINE, "a.Foo-Debug"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.INFO, "a.Foo-Info"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.WARNING, "a.Foo-Warn"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.SEVERE, "a.Foo-Error", fooException), logSaver);

        verifyNotLogged(ExpectedLogs.matcher(Level.FINEST, "a.Foo-Trace"), logSaver);
        verifyNotLogged(ExpectedLogs.matcher(Level.FINE, "a.b.Bar-Debug"), logSaver);
        verifyNotLogged(ExpectedLogs.matcher(Level.INFO, "a.b.Bar-Info"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.WARNING, "a.b.Bar-Warn"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.SEVERE, "a.b.Bar-Error", barException), logSaver);

        verifyLogged(ExpectedLogs.matcher(Level.INFO, "SLF4J log messages work"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.INFO, "JCL log messages work"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.INFO, "JUL log messages work"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.INFO, "Log4j log messages work"), logSaver);
        verifyLogged(ExpectedLogs.matcher(Level.INFO, "Log4j2 log messages work"), logSaver);
        output.output(element);
      } finally {
        LogManager.getLogManager().getLogger("").removeHandler(logSaver);
      }
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesSdkHarnessEnvironment.class})
  public void testLogging() throws Exception {
    p.getOptions().as(SdkHarnessOptions.class).setDefaultSdkHarnessLogLevel(LogLevel.DEBUG);
    p.getOptions()
        .as(SdkHarnessOptions.class)
        .setSdkHarnessLogLevelOverrides(
            new SdkHarnessLogLevelOverrides().addOverrideForName("a.b.Bar", LogLevel.WARN));
    PCollection<String> input = p.apply(Create.of("Logging Works").withCoder(StringUtf8Coder.of()));
    PCollection<String> output = input.apply(ParDo.of(new LoggingDoFn()));
    PAssert.that(output).containsInAnyOrder("Logging Works");
    p.run().waitUntilFinish();
  }
}
