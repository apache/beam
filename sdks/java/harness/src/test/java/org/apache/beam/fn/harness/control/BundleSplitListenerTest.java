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
package org.apache.beam.fn.harness.control;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BundleSplitListener}. */
@RunWith(JUnit4.class)
public class BundleSplitListenerTest {
  private static final BundleApplication TEST_PRIMARY_1 =
      BundleApplication.newBuilder().setInputId("primary1").build();
  private static final BundleApplication TEST_PRIMARY_2 =
      BundleApplication.newBuilder().setInputId("primary2").build();
  private static final DelayedBundleApplication TEST_RESIDUAL_1 =
      DelayedBundleApplication.newBuilder()
          .setApplication(BundleApplication.newBuilder().setInputId("residual1").build())
          .build();
  private static final DelayedBundleApplication TEST_RESIDUAL_2 =
      DelayedBundleApplication.newBuilder()
          .setApplication(BundleApplication.newBuilder().setInputId("residual2").build())
          .build();

  @Test
  public void testInMemory() {
    BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();
    splitListener.split(
        Collections.singletonList(TEST_PRIMARY_1), Collections.singletonList(TEST_RESIDUAL_1));
    splitListener.split(
        Collections.singletonList(TEST_PRIMARY_2), Collections.singletonList(TEST_RESIDUAL_2));
    assertThat(splitListener.getPrimaryRoots(), contains(TEST_PRIMARY_1, TEST_PRIMARY_2));
    assertThat(splitListener.getResidualRoots(), contains(TEST_RESIDUAL_1, TEST_RESIDUAL_2));
    splitListener.clear();
    assertThat(splitListener.getPrimaryRoots(), empty());
    assertThat(splitListener.getResidualRoots(), empty());
  }
}
