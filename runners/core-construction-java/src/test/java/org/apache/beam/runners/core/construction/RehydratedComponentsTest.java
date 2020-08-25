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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RehydratedComponents}.
 *
 * <p>These are basic sanity checks. The most thorough testing of this is by extensive use in all
 * other rehydration. The two are tightly coupled, as they recursively invoke each other.
 */
@RunWith(JUnit4.class)
public class RehydratedComponentsTest {

  @Test
  public void testSimpleCoder() throws Exception {
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    Coder<?> coder = VarIntCoder.of();
    String id = sdkComponents.registerCoder(coder);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());

    Coder<?> rehydratedCoder = rehydratedComponents.getCoder(id);
    assertThat(rehydratedCoder, equalTo((Coder) coder));
    assertThat(rehydratedComponents.getCoder(id), theInstance((Coder) rehydratedCoder));
  }

  @Test
  public void testCompoundCoder() throws Exception {
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    Coder<?> coder = VarIntCoder.of();
    Coder<?> compoundCoder = NullableCoder.of(coder);
    String compoundCoderId = sdkComponents.registerCoder(compoundCoder);
    String coderId = sdkComponents.registerCoder(coder);

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());

    Coder<?> rehydratedCoder = rehydratedComponents.getCoder(coderId);
    Coder<?> rehydratedCompoundCoder = rehydratedComponents.getCoder(compoundCoderId);

    assertThat(rehydratedCoder, equalTo((Coder) coder));
    assertThat(rehydratedCompoundCoder, equalTo((Coder) compoundCoder));

    assertThat(rehydratedComponents.getCoder(coderId), theInstance((Coder) rehydratedCoder));
    assertThat(
        rehydratedComponents.getCoder(compoundCoderId),
        theInstance((Coder) rehydratedCompoundCoder));
  }

  @Test
  public void testWindowingStrategy() throws Exception {
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    WindowingStrategy windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(1)))
            .withAllowedLateness(Duration.standardSeconds(4));
    String id = sdkComponents.registerWindowingStrategy(windowingStrategy);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());

    WindowingStrategy<?, ?> rehydratedStrategy = rehydratedComponents.getWindowingStrategy(id);
    assertThat(
        rehydratedStrategy,
        equalTo(
            (WindowingStrategy)
                windowingStrategy
                    .withEnvironmentId(sdkComponents.getOnlyEnvironmentId())
                    .fixDefaults()));
    assertThat(
        rehydratedComponents.getWindowingStrategy(id),
        theInstance((WindowingStrategy) rehydratedStrategy));
  }

  @Test
  public void testEnvironment() {
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    Environment env = Environments.createDockerEnvironment("java_test");
    String id = sdkComponents.registerEnvironment(env);
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());

    Environment rehydratedEnv = rehydratedComponents.getEnvironment(id);
    assertThat(rehydratedEnv, equalTo(env));
    assertThat(rehydratedComponents.getEnvironment(id), theInstance(rehydratedEnv));
  }
}
