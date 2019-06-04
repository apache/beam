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
package org.apache.beam.runners.apex.translation;

import com.datatorrent.api.DAG;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner.CreateApexPCollectionView;
import org.apache.beam.runners.apex.translation.operators.ApexProcessFnOperator;
import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.core.construction.PrimitiveCreate;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ApexPipelineTranslator} translates {@link Pipeline} objects into Apex logical plan {@link
 * DAG}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ApexPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {
  private static final Logger LOG = LoggerFactory.getLogger(ApexPipelineTranslator.class);

  /**
   * A map from {@link PTransform} subclass to the corresponding {@link TransformTranslator} to use
   * to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, TransformTranslator> transformTranslators =
      new HashMap<>();

  private final TranslationContext translationContext;

  static {
    // register TransformTranslators
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoTranslator<>());
    registerTransformTranslator(
        SplittableParDoViaKeyedWorkItems.ProcessElements.class,
        new ParDoTranslator.SplittableProcessElementsTranslator());
    registerTransformTranslator(GBKIntoKeyedWorkItems.class, new GBKIntoKeyedWorkItemsTranslator());
    registerTransformTranslator(Read.Unbounded.class, new ReadUnboundedTranslator());
    registerTransformTranslator(Read.Bounded.class, new ReadBoundedTranslator());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator());
    registerTransformTranslator(Flatten.PCollections.class, new FlattenPCollectionTranslator());
    registerTransformTranslator(PrimitiveCreate.class, new CreateValuesTranslator());
    registerTransformTranslator(
        CreateApexPCollectionView.class, new CreateApexPCollectionViewTranslator());
    registerTransformTranslator(CreatePCollectionView.class, new CreatePCollectionViewTranslator());
    registerTransformTranslator(Window.Assign.class, new WindowAssignTranslator());
  }

  public ApexPipelineTranslator(ApexPipelineOptions options) {
    this.translationContext = new TranslationContext(options);
  }

  public void translate(Pipeline pipeline, DAG dag) {
    pipeline.traverseTopologically(this);
    translationContext.populateDAG(dag);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug("entering composite transform {}", node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug("leaving composite transform {}", node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.debug("visiting transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    TransformTranslator translator = getTransformTranslator(transform.getClass());
    if (null == translator) {
      throw new UnsupportedOperationException("no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    translator.translate(transform, translationContext);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    LOG.debug("visiting value {}", value);
  }

  /**
   * Records that instances of the specified PTransform class should be translated by default by the
   * corresponding {@link TransformTranslator}.
   */
  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }

  /**
   * Returns the {@link TransformTranslator} to use for instances of the specified PTransform class,
   * or null if none registered.
   */
  private <TransformT extends PTransform<?, ?>>
      TransformTranslator<TransformT> getTransformTranslator(Class<TransformT> transformClass) {
    return transformTranslators.get(transformClass);
  }

  private static class ReadBoundedTranslator<T> implements TransformTranslator<Read.Bounded<T>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void translate(Read.Bounded<T> transform, TranslationContext context) {
      // TODO: adapter is visibleForTesting
      BoundedToUnboundedSourceAdapter unboundedSource =
          new BoundedToUnboundedSourceAdapter<>(transform.getSource());
      ApexReadUnboundedInputOperator<T, ?> operator =
          new ApexReadUnboundedInputOperator<>(unboundedSource, true, context.getPipelineOptions());
      context.addOperator(operator, operator.output);
    }
  }

  private static class CreateApexPCollectionViewTranslator<ElemT, ViewT>
      implements TransformTranslator<CreateApexPCollectionView<ElemT, ViewT>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void translate(
        CreateApexPCollectionView<ElemT, ViewT> transform, TranslationContext context) {
      context.addView(transform.getView());
      LOG.debug("view {}", transform.getView().getName());
    }
  }

  private static class CreatePCollectionViewTranslator<ElemT, ViewT>
      implements TransformTranslator<CreatePCollectionView<ElemT, ViewT>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void translate(
        CreatePCollectionView<ElemT, ViewT> transform, TranslationContext context) {
      context.addView(transform.getView());
      LOG.debug("view {}", transform.getView().getName());
    }
  }

  private static class GBKIntoKeyedWorkItemsTranslator<K, InputT>
      implements TransformTranslator<GBKIntoKeyedWorkItems<K, InputT>> {

    @Override
    public void translate(GBKIntoKeyedWorkItems<K, InputT> transform, TranslationContext context) {
      // https://issues.apache.org/jira/browse/BEAM-1850
      ApexProcessFnOperator<KV<K, InputT>> operator =
          ApexProcessFnOperator.toKeyedWorkItems(context.getPipelineOptions());
      context.addOperator(operator, operator.outputPort);
      context.addStream(context.getInput(), operator.inputPort);
    }
  }
}
