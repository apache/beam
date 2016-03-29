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

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowPipelineDebugOptions}. */
@RunWith(JUnit4.class)
public class DataflowPipelineDebugOptionsTest {
  @Test
  public void testTransformNameMapping() throws Exception {
    DataflowPipelineDebugOptions options = PipelineOptionsFactory
        .fromArgs(new String[]{"--transformNameMapping={\"a\":\"b\",\"foo\":\"\",\"bar\":\"baz\"}"})
        .as(DataflowPipelineDebugOptions.class);
    assertEquals(3, options.getTransformNameMapping().size());
    assertThat(options.getTransformNameMapping(), hasEntry("a", "b"));
    assertThat(options.getTransformNameMapping(), hasEntry("foo", ""));
    assertThat(options.getTransformNameMapping(), hasEntry("bar", "baz"));
  }
}
