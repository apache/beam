/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.ApproximateUnique;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.junit.Test;

public class WordCountTest {

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWordsFn extends DoFn<String, String> {

    private TupleTag<String> regex;

    public ExtractWordsFn(TupleTag<String> regex) {
      this.regex = regex;
    }
    @Override
    public void processElement(ProcessContext c) {
      String[] words = c.element().split(c.sideInput(regex));
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    private final PObject<String> regex;

    public CountWords(PObject<String> regex) {
      this.regex = regex;
    }

    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {
      // Convert lines of text into individual words.
      TupleTag<String> tag = new TupleTag<String>();
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn(tag))
          .withSideInputs(PObjectTuple.of(tag, regex)));
      // Count the number of times each word occurs.
      return words.apply(Count.<String>create());
    }
  }

  @Test
  public void testRun() throws Exception {
    Pipeline p = Pipeline.create(new PipelineOptions());
    PObject<String> regex = p.apply(CreatePObject.of("[^a-zA-Z']+"));
    PCollection<String> w1 = p.apply(Create.of("Here are some words to count"));
    PCollection<String> w2 = p.apply(Create.of("Here are some more words"));
    PCollectionList<String> list = PCollectionList.of(w1).and(w2);

    PCollection<String> union = list.apply(Flatten.<String>create());
    PCollection<KV<String, Long>> counts = union.apply(new CountWords(regex));
    PObject<Long> unique = counts.apply(ApproximateUnique.<KV<String, Long>>globally(16));

    EvaluationResult res = new SparkPipelineRunner("local[2]").run(p);
    System.out.println(res.get(counts));
    System.out.println(res.get(unique));
  }
}
