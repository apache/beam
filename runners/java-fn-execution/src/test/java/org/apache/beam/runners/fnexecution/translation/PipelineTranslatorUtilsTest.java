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
package org.apache.beam.runners.fnexecution.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/** Tests for {@link PipelineTranslatorUtils}. */
public class PipelineTranslatorUtilsTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Test
  public void testOutputMapCreation() {
    List<String> outputs = Arrays.asList("output1", "output2", "output3");
    BiMap<String, Integer> outputMap = PipelineTranslatorUtils.createOutputMap(outputs);
    Map<Object, Object> expected =
        ImmutableMap.builder().put("output1", 0).put("output2", 1).put("output3", 2).build();
    assertThat(outputMap, is(expected));
  }
}
