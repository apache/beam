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
package org.apache.beam.runners.jet;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/** Slightly altered version of the Jet based runner, used in unit-tests. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class TestJetRunner extends PipelineRunner<PipelineResult> {

  /** A map from a Transform URN to the translator. */
  private static final Map<String, JetTransformTranslator> TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator());
  }

  private final JetTestInstanceFactory factory;
  private final JetRunner delegate;

  private TestJetRunner(PipelineOptions options) {
    this.factory = new JetTestInstanceFactory();

    this.delegate = JetRunner.fromOptions(options, factory::newClient);
    this.delegate.addExtraTranslators(TestJetRunner::getTranslator);
  }

  public static TestJetRunner fromOptions(PipelineOptions options) {
    return new TestJetRunner(options);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    Collection<JetInstance> instances = initMemberInstances(factory);
    try {
      PipelineResult result = delegate.run(pipeline);
      if (result instanceof FailedRunningPipelineResults) {
        throw ((FailedRunningPipelineResults) result).getCause();
      }
      result.waitUntilFinish();
      return result;
    } finally {
      killMemberInstances(instances, factory);
    }
  }

  private static Collection<JetInstance> initMemberInstances(
      JetTestInstanceFactory internalFactory) {
    JetConfig config =
        new JetConfig()
            .configureHazelcast(
                c ->
                    c.addMapConfig(
                        new MapConfig("map")
                            .setEventJournalConfig(new EventJournalConfig().setEnabled(true))));

    return Arrays.asList(internalFactory.newMember(config), internalFactory.newMember(config));
  }

  private static void killMemberInstances(
      Collection<JetInstance> instances, JetTestInstanceFactory internalFactory) {
    if (!instances.isEmpty()) {
      // there are own member instances to kill
      internalFactory.shutdownAll();
    }
  }

  private static JetTransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
    String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSLATORS.get(urn);
  }

  private static class TestStreamTranslator<T>
      implements JetTransformTranslator<PTransform<PBegin, PCollection<T>>> {
    @Override
    public Vertex translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        JetTranslationContext context) {
      String transformName = appliedTransform.getFullName();
      DAGBuilder dagBuilder = context.getDagBuilder();
      String vertexId = dagBuilder.newVertexId(transformName);

      TestStream<T> testStream = (TestStream<T>) appliedTransform.getTransform();

      // events in the transform are not serializable, we have to translate them. We'll also flatten
      // the collection.
      Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
      Coder outputCoder = Utils.getCoder((PCollection) output.getValue());
      TestStream.TestStreamCoder<T> payloadCoder =
          TestStream.TestStreamCoder.of(testStream.getValueCoder());
      byte[] encodedPayload = getEncodedPayload(testStream, payloadCoder);
      Vertex vertex =
          dagBuilder.addVertex(
              vertexId, TestStreamP.supplier(encodedPayload, payloadCoder, outputCoder));

      String outputEdgeId = Utils.getTupleTagId(output.getValue());
      dagBuilder.registerCollectionOfEdge(outputEdgeId, output.getKey().getId());
      dagBuilder.registerEdgeStartPoint(outputEdgeId, vertex, outputCoder);
      return vertex;
    }

    private static <T> byte[] getEncodedPayload(
        TestStream<T> testStream, TestStream.TestStreamCoder<T> coder) {
      try {
        return CoderUtils.encodeToByteArray(coder, testStream);
      } catch (CoderException e) {
        throw ExceptionUtil.rethrow(e);
      }
    }
  }
}
