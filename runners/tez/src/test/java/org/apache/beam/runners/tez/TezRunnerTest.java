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
package org.apache.beam.runners.tez;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the Tez runner.
 */
public class TezRunnerTest {

  private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
  private static final String INPUT_LOCATION = "src/test/resources/test_input.txt";

  private static Pipeline tezPipeline;
  private static Pipeline directPipeline;

  @Before
  public void setupPipelines(){
    //TezRunner Pipeline
    PipelineOptions tezOptions = PipelineOptionsFactory.create();
    tezOptions.setRunner(TezRunner.class);
    tezPipeline = Pipeline.create(tezOptions);

    //DirectRunner Pipeline
    PipelineOptions options = PipelineOptionsFactory.create();
    directPipeline = Pipeline.create(options);
  }

  @Test
  public void simpleTest() throws Exception {
    tezPipeline.apply(TextIO.read().from(INPUT_LOCATION))
        .apply(ParDo.of(new AddHelloWorld()))
        .apply(ParDo.of(new TestTezFn()));

    directPipeline.apply(TextIO.read().from(INPUT_LOCATION))
        .apply(ParDo.of(new AddHelloWorld()))
        .apply(ParDo.of(new TestDirectFn()));

    tezPipeline.run().waitUntilFinish();
    directPipeline.run().waitUntilFinish();
    Assert.assertEquals(TestDirectFn.RESULTS, TestTezFn.RESULTS);
  }

  @Test
  public void wordCountTest() throws Exception {
    tezPipeline.apply("ONE", TextIO.read().from(INPUT_LOCATION))
        .apply("TWO", ParDo.of(new TokenDoFn()))
        .apply("THREE", GroupByKey.create())
        .apply("FOUR", ParDo.of(new ProcessDoFn()))
        .apply("FIVE", ParDo.of(new TestTezFn()));

    directPipeline.apply("ONE", TextIO.read().from(INPUT_LOCATION))
        .apply("TWO", ParDo.of(new TokenDoFn()))
        .apply("THREE", GroupByKey.create())
        .apply("FOUR", ParDo.of(new ProcessDoFn()))
        .apply("FIVE", ParDo.of(new TestDirectFn()));

    tezPipeline.run().waitUntilFinish();
    directPipeline.run().waitUntilFinish();
    Assert.assertEquals(TestDirectFn.RESULTS, TestTezFn.RESULTS);
  }

  private static class AddHelloWorld extends DoFn<String, String>{
    @ProcessElement
    public void processElement(ProcessContext c) {

      // Split the line into words.
      String[] words = c.element().split(TOKENIZER_PATTERN);
      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word + " HelloWorld");
        }
      }
    }
  }

  public static class TokenDoFn extends DoFn<String, KV<String, Integer>>{
    @ProcessElement
    public void processElement(ProcessContext c){
      for( String word : c.element().split(TOKENIZER_PATTERN)){
        if(!word.isEmpty()){
          c.output(KV.of(word, 1));
        }
      }
    }
  }

  public static class ProcessDoFn extends DoFn<KV<String,Iterable<Integer>>, String>{
    @ProcessElement
    public void processElement(ProcessContext c){
      Integer sum = 0;
      for( Integer integer : c.element().getValue()){
        sum = sum + integer;
      }
      c.output(c.element().getKey() + ": " + sum);
    }
  }

  private static class TestTezFn extends DoFn<String, String> {
    private static final Set<String> RESULTS = Collections.synchronizedSet(new HashSet<>());

    public TestTezFn(){
      RESULTS.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      RESULTS.add(c.element());
    }
  }

  private static class TestDirectFn extends DoFn<String, String> {
    private static final Set<String> RESULTS = Collections.synchronizedSet(new HashSet<>());

    public TestDirectFn(){
      RESULTS.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      RESULTS.add(c.element());
    }
  }

}
