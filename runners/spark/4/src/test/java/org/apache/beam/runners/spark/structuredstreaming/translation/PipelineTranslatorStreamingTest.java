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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.StreamingTestUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Verifies that unsupported transforms in streaming mode fail at translation time. */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class PipelineTranslatorStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();
  @Rule public transient TemporaryFolder temp = new TemporaryFolder();

  private PCollection<KV<String, Integer>> kv(String tag) throws Exception {
    SparkStructuredStreamingPipelineOptions o = StreamingTestUtils.streamingOptions(temp);
    return Pipeline.create(o)
        .apply(Read.from(new TestUnboundedSource(tag, 1, 1)))
        .apply(Window.into(FixedWindows.of(Duration.millis(1))))
        .apply(ParDo.of(new ToKvFn()));
  }

  private static void assertUnsupported(Pipeline pipeline, String expected) {
    Throwable thrown = assertThrows(Exception.class, () -> StreamingTestUtils.run(pipeline));
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof UnsupportedOperationException && t.getMessage().contains(expected)) {
        return;
      }
    }
    throw new AssertionError("missing " + expected);
  }

  @Test
  public void rejectsGroupByKey() throws Exception {
    assertUnsupported(kv("g").apply(GroupByKey.create()).getPipeline(), "GroupByKey");
  }

  @Test
  public void rejectsCombinePerKey() throws Exception {
    assertUnsupported(
        kv("c").apply(Combine.perKey(Sum.ofIntegers())).getPipeline(), "Combine.perKey");
  }

  @Test
  public void rejectsImpulseFromCreate() throws Exception {
    SparkStructuredStreamingPipelineOptions o = StreamingTestUtils.streamingOptions(temp);
    Pipeline p = Pipeline.create(o);
    p.apply(Create.of("rejected"));
    assertUnsupported(p, "Impulse");
  }

  @Test
  public void rejectsBoundedReadFromCreate() throws Exception {
    SparkStructuredStreamingPipelineOptions o = StreamingTestUtils.streamingOptions(temp);
    Pipeline p = Pipeline.create(o);
    p.apply(Create.of("rejected", "too"));
    assertUnsupported(p, "Bounded Read");
  }

  @Test
  public void rejectsStatefulParDo() throws Exception {
    assertUnsupported(kv("s").apply(ParDo.of(new StatefulDoFn())).getPipeline(), "Stateful ParDo");
  }

  @Test
  public void rejectsAdditionalOutputs() throws Exception {
    TupleTag<Integer> main = new TupleTag<Integer>() {};
    TupleTag<Integer> other = new TupleTag<Integer>() {};
    assertUnsupported(
        kv("o")
            .apply(ParDo.of(new StatelessDoFn()).withOutputTags(main, TupleTagList.of(other)))
            .getPipeline(),
        "additional outputs");
  }

  private static final class ToKvFn extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void process(@Element String element, OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of(element, 1));
    }
  }

  private static final class StatelessDoFn extends DoFn<KV<String, Integer>, Integer> {
    @ProcessElement
    public void process() {}
  }

  private static final class StatefulDoFn extends DoFn<KV<String, Integer>, Integer> {
    @DoFn.StateId("state")
    final StateSpec<?> spec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void process() {}
  }
}
