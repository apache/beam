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

package org.apache.beam.runners.dataflow;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.BatchStatefulParDoOverrides.StatefulMultiOutputParDo;
import org.apache.beam.runners.dataflow.BatchStatefulParDoOverrides.StatefulSingleOutputParDo;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for {@link BatchStatefulParDoOverrides}. */
@RunWith(JUnit4.class)
public class BatchStatefulParDoOverridesTest implements Serializable {

  @Test
  public void testSingleOutputOverrideNonCrashing() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    DummyStatefulDoFn fn = new DummyStatefulDoFn();
    pipeline.apply(Create.of(KV.of(1, 2))).apply(ParDo.of(fn));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceTransforms(pipeline);
    assertThat(findBatchStatefulDoFn(pipeline), equalTo((DoFn) fn));
  }

  @Test
  public void testMultiOutputOverrideNonCrashing() throws Exception {
    DataflowPipelineOptions options = buildPipelineOptions();
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};

    DummyStatefulDoFn fn = new DummyStatefulDoFn();
    pipeline
        .apply(Create.of(KV.of(1, 2)))
        .apply(ParDo.of(fn).withOutputTags(mainOutputTag, TupleTagList.empty()));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    runner.replaceTransforms(pipeline);
    assertThat(findBatchStatefulDoFn(pipeline), equalTo((DoFn) fn));
  }

  private static DummyStatefulDoFn findBatchStatefulDoFn(Pipeline p) {
    FindBatchStatefulDoFnVisitor findBatchStatefulDoFnVisitor = new FindBatchStatefulDoFnVisitor();
    p.traverseTopologically(findBatchStatefulDoFnVisitor);
    return (DummyStatefulDoFn) findBatchStatefulDoFnVisitor.getStatefulDoFn();
  }

  private static class DummyStatefulDoFn extends DoFn<KV<Integer, Integer>, Integer> {

    @StateId("foo")
    private final StateSpec<ValueState<Integer>> spec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElem(ProcessContext c) {
      // noop
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof DummyStatefulDoFn;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  private static class FindBatchStatefulDoFnVisitor extends PipelineVisitor.Defaults {

    @Nullable private DoFn<?, ?> batchStatefulDoFn;

    public DoFn<?, ?> getStatefulDoFn() {
      assertThat(batchStatefulDoFn, not(nullValue()));
      return batchStatefulDoFn;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      if (node.getTransform() instanceof StatefulSingleOutputParDo) {
        batchStatefulDoFn =
            ((StatefulSingleOutputParDo) node.getTransform()).getOriginalParDo().getFn();
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      } else if (node.getTransform() instanceof StatefulMultiOutputParDo) {
        batchStatefulDoFn =
            ((StatefulMultiOutputParDo) node.getTransform()).getOriginalParDo().getFn();
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      } else {
        return CompositeBehavior.ENTER_TRANSFORM;
      }
    }
  }

  private static DataflowPipelineOptions buildPipelineOptions() throws IOException {
    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.expand(any(GcsPath.class))).then(new Answer<List<GcsPath>>() {
      @Override
      public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
      }
    });
    when(mockGcsUtil.bucketAccessible(any(GcsPath.class))).thenReturn(true);

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setRunner(DataflowRunner.class);
    options.setGcpCredential(new TestCredential());
    options.setJobName("some-job-name");
    options.setProject("some-project");
    options.setTempLocation(GcsPath.fromComponents("somebucket", "some/path").toString());
    options.setFilesToStage(new LinkedList<String>());
    options.setGcsUtil(mockGcsUtil);
    return options;
  }
}
