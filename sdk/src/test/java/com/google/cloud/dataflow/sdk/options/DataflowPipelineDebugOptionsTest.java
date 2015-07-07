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

import static com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions.NameMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowPipelineDebugOptions}. */
@RunWith(JUnit4.class)
public class DataflowPipelineDebugOptionsTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testTransformNameMapping() throws Exception {
    NameMap map = MAPPER.convertValue("a=b;foo=;bar=baz", NameMap.class);
    assertEquals(3, map.size());
    assertThat(map, hasEntry("a", "b"));
    assertThat(map, hasEntry("foo", ""));
    assertThat(map, hasEntry("bar", "baz"));
  }
}
