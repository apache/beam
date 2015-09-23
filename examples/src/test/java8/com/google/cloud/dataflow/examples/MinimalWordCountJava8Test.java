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

package com.google.cloud.dataflow.examples;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;

/**
 * To keep {@link MinimalWordCountJava8} simple, it is not factored or testable. This test
 * file should be maintained with a copy of its code for a basic smoke test.
 */
@RunWith(JUnit4.class)
public class MinimalWordCountJava8Test implements Serializable {

  /**
   * A basic smoke test that ensures there is no crash at pipeline construction time.
   */
  @Test
  public void testMinimalWordCountJava8() {
    Pipeline p = TestPipeline.create();

    p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
     .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
         .withOutputType(new TypeDescriptor<String>() {}))
     .apply(Filter.byPredicate((String word) -> !word.isEmpty()))
     .apply(Count.<String>perElement())
     .apply(MapElements
         .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue())
         .withOutputType(new TypeDescriptor<String>() {}))
     .apply(TextIO.Write.to("gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX"));
  }
}
