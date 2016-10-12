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

package org.apache.beam.runners.spark;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.RemoveDuplicates;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * A test based on {@code DeDupExample} from the SDK.
 */
public class DeDupTest {

  private static final String[] LINES_ARRAY = {
      "hi there", "hello", "hi there",
      "hi", "hello"};
  private static final List<String> LINES = Arrays.asList(LINES_ARRAY);
  private static final Set<String> EXPECTED_SET =
      ImmutableSet.of("hi there", "hi", "hello");

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply(Create.of(LINES).withCoder(StringUtf8Coder.of()));
    PCollection<String> output = input.apply(RemoveDuplicates.<String>create());

    PAssert.that(output).containsInAnyOrder(EXPECTED_SET);

    p.run();
  }
}
