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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.extensions.euphoria.core.translate.EuphoriaOptions;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TranslatorProvider;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Utility class for easier creating input data sets for operator testing. */
public class TestUtils {

  private static class PrimitiveOutputTranslatorProvider implements TranslatorProvider {

    @Override
    public <InputT, OutputT, OperatorT extends Operator<OutputT>>
        Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(
            OperatorT operator) {
      return Optional.of(
          (op, inputs) ->
              PCollection.<OutputT>createPrimitiveOutputInternal(
                      inputs.getPipeline(),
                      inputs.get(0).getWindowingStrategy(),
                      inputs.get(0).isBounded(),
                      null)
                  .setTypeDescriptor(TypeAwareness.orObjects(operator.getOutputType())));
    }
  }

  public static TestPipeline createTestPipeline() {
    final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions
        .as(EuphoriaOptions.class)
        .setTranslatorProvider(new PrimitiveOutputTranslatorProvider());
    final TestPipeline testPipeline = TestPipeline.fromOptions(pipelineOptions);
    testPipeline
        .getCoderRegistry()
        .registerCoderForClass(Object.class, KryoCoder.of(pipelineOptions));
    return testPipeline;
  }

  public static <T> PCollection<T> createMockDataset(TypeDescriptor<T> typeDescriptor) {
    return createMockDataset(createTestPipeline(), typeDescriptor);
  }

  public static <T> PCollection<T> createMockDataset(
      Pipeline pipeline, TypeDescriptor<T> typeDescriptor) {
    return pipeline.apply(Create.empty(typeDescriptor));
  }

  /**
   * Get an {@link Operator} that produced a given {@link PCollection}.
   *
   * @param pCollection pCollection to find producer for
   * @param <T> type of produced data
   * @return producer
   */
  public static <T> Operator<T> getProducer(PCollection<T> pCollection) {
    final AtomicReference<Operator<T>> operator = new AtomicReference<>();
    pCollection
        .getPipeline()
        .traverseTopologically(
            new Pipeline.PipelineVisitor() {

              @Override
              public void enterPipeline(Pipeline p) {
                // noop
              }

              @Override
              public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
                if (node.getTransform() instanceof OperatorTransform) {
                  // we do not want to expand euphoria transforms
                  return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
                }
                return CompositeBehavior.ENTER_TRANSFORM;
              }

              @Override
              public void leaveCompositeTransform(TransformHierarchy.Node node) {
                // noop
              }

              @Override
              public void visitPrimitiveTransform(TransformHierarchy.Node node) {
                // noop
              }

              @Override
              public void visitValue(PValue value, TransformHierarchy.Node producer) {
                if (value.equals(pCollection)) {
                  @SuppressWarnings("unchecked")
                  OperatorTransform<?, T, Operator<T>> transform =
                      requireNonNull((OperatorTransform) producer.getTransform());
                  operator.set(transform.getOperator());
                }
              }

              @Override
              public void leavePipeline(Pipeline pipeline) {
                // noop
              }
            });
    return requireNonNull(
        operator.get(), "Can not find producer for PCollection [" + pCollection.getName() + "].");
  }
}
