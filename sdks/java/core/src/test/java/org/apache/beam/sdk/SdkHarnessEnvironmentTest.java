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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;

import java.security.Security;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
      MemoryMeter memoryMeter =
          MemoryMeter.builder().withGuessing(Guess.ALWAYS_INSTRUMENTATION).build();
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
}
