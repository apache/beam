/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EmptyInputTest {

  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    List<String> empty = Collections.emptyList();
    PCollection<String> inputWords = p.apply(Create.of(empty)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(Combine.globally(new ConcatWords()));

    EvaluationResult res = SparkPipelineRunner.create().run(p);
    assertEquals("", Iterables.getOnlyElement(res.get(output)));
    res.close();
  }

  public static class ConcatWords implements SerializableFunction<Iterable<String>, String> {
    @Override
    public String apply(Iterable<String> input) {
      StringBuilder all = new StringBuilder();
      for (String item : input) {
        if (!item.isEmpty()) {
          if (all.length() == 0) {
            all.append(item);
          } else {
            all.append(",");
            all.append(item);
          }
        }
      }
      return all.toString();
    }
  }

}
