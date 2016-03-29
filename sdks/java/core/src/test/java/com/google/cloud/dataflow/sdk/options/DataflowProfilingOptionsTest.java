/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DataflowProfilingOptions}.
 */
@RunWith(JUnit4.class)
public class DataflowProfilingOptionsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testOptionsObject() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(new String[] {
        "--enableProfilingAgent", "--profilingAgentConfiguration={\"interval\": 21}"})
        .as(DataflowPipelineOptions.class);
    assertTrue(options.getEnableProfilingAgent());

    String json = MAPPER.writeValueAsString(options);
    assertThat(json, Matchers.containsString(
        "\"profilingAgentConfiguration\":{\"interval\":21}"));
  }
}
