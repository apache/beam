/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.dataflow;

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataflowUtils}. */
@RunWith(JUnit4.class)
public class DataflowUtilsTest {
  @Test
  public void testCreateJobName() {
    String name = "create-job-name";
    assertThat(createJobName(name)).matches(name + "-\\d{14}");
  }

  @Test
  public void testCreateJobNameWithUppercase() {
    assertThat(createJobName("testWithUpperCase")).matches("test-with-upper-case" + "-\\d{14}");
  }
}
