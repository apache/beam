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
package org.apache.beam.it.common.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;
import static org.apache.beam.it.common.utils.PipelineUtils.extractJobName;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PipelineUtils}. */
@RunWith(JUnit4.class)
public class PipelineUtilsTest {
  @Test
  public void testCreateJobName() {
    String name = "create-job-name";
    assertThat(createJobName(name)).matches(name + "-\\d{17}");
  }

  @Test
  public void testCreateJobNameWithUppercase() {
    assertThat(createJobName("testWithUpperCase")).matches("test-with-upper-case-\\d{17}");
  }

  @Test
  public void testCreateJobNameWithUppercaseSuffix() {
    assertThat(createJobName("testWithUpperCase", 8))
        .matches("test-with-upper-case-\\d{17}-[a-z0-9]{8}");
  }

  @Test
  public void testCreateExtractJobName() {
    String name = "create-job-name";
    assertEquals(name, extractJobName(createJobName(name)));
  }

  @Test
  public void testCreateExtractJobNameWithRandomChars() {
    String name = "create-job-name";
    assertEquals(name, extractJobName(createJobName(name, 8)));
  }
}
