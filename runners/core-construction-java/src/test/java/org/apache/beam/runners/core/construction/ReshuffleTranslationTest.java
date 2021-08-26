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

import static org.apache.beam.runners.core.construction.PTransformTranslation.RESHUFFLE_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.transforms.Reshuffle;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReshuffleTranslation}. */
@RunWith(JUnit4.class)
public class ReshuffleTranslationTest {

  /**
   * Tests that the translator is registered so the URN can be retrieved (the only thing you can
   * meaningfully do with a {@link Reshuffle}).
   */
  @Test
  public void testUrnRetrievable() throws Exception {
    assertThat(PTransformTranslation.urnForTransform(Reshuffle.of()), equalTo(RESHUFFLE_URN));
  }
}
