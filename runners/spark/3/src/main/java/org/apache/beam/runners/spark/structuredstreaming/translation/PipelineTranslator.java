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

import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.SideInputValues;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

/**
 * The pipeline translator translates a Beam {@link Pipeline} into a Spark correspondence, that can
 * then be evaluated.
 *
 * <p>The translation involves traversing the hierarchy of a pipeline multiple times:
 *
 * <ol>
 *   <li>Detect if {@link StreamingOptions#setStreaming streaming} mode is required.
 *   <li>Identify datasets that are repeatedly used as input and should be cached.
 *   <li>And finally, translate each primitive or composite {@link PTransform} that is {@link
 *       #getTransformTranslator known} and {@link TransformTranslator#canTranslate supported} into
 *       its Spark correspondence. If a composite is not supported, it will be expanded further into
 *       its parts and translated then.
 * </ol>
 */
@Internal
public abstract class PipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class);

  public static void replaceTransforms(Pipeline pipeline, StreamingOptions options) {
    pipeline.replaceAll(SparkTransformOverrides.getDefaultOverrides(options.isStreaming()));
  }

  /**
   * Analyse the pipeline to determine if we have to switch to streaming mode for the pipeline
   * translation and update {@link StreamingOptions} accordingly.
   */
  public static void detectStreamingMode(Pipeline pipeline, StreamingOptions options) {
    StreamingModeDetector detector = new StreamingModeDetector(options.isStreaming());
    pipeline.traverseTopologically(detector);
    options.setStreaming(detector.streaming);
  }

  /** Returns a {@link TransformTranslator} for the given {@link PTransform} if known. */
  protected abstract @Nullable <
          InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform);

  /**
   * Translates a Beam pipeline into its Spark correspondence using the Spark SQL / Dataset API.
   *
   * <p>Note, in some cases this involves the early evaluation of some parts of the pipeline. For
   * example, in order to use a side-input {@link org.apache.beam.sdk.values.PCollectionView
   * PCollectionView} in a translation the corresponding Spark {@link
   * org.apache.beam.runners.spark.translation.Dataset Dataset} might have to be collected and
   * broadcasted to be able to continue with the translation.
   *
   * @return The result of the translation is an {@link EvaluationContext} that can trigger the
   *     evaluation of the Spark pipeline.
   */
  public EvaluationContext translate(
      Pipeline pipeline, SparkSession session, SparkCommonPipelineOptions options) {
    LOG.debug("starting translation of the pipeline using {}", getClass().getName());
    DependencyVisitor dependencies = new DependencyVisitor();
    pipeline.traverseTopologically(dependencies);

    TranslatingVisitor translator = new TranslatingVisitor(session, options, dependencies.results);
    pipeline.traverseTopologically(translator);

    return new EvaluationContext(translator.leaves, session);
  }

  /**
   * The correspondence of a {@link PCollection} as result of translating a {@link PTransform}
   * including additional metadata (such as name and dependents).
   */
  private static final class TranslationResult<T> implements EvaluationContext.NamedDataset<T> {
    private final String name;
    private @MonotonicNonNull Dataset<WindowedValue<T>> dataset = null;
    private @MonotonicNonNull Broadcast<SideInputValues<T>> sideInputBroadcast = null;
    private final Set<PTransform<?, ?>> dependentTransforms = new HashSet<>();

    private TranslationResult(PCollection<?> pCol) {
      this.name = pCol.getName();
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public @Nullable Dataset<WindowedValue<T>> dataset() {
      return dataset;
    }
  }

  /** Shared, mutable state during the translation of a pipeline and omitted afterwards. */
  public interface TranslationState extends EncoderProvider {
    <T> Dataset<WindowedValue<T>> getDataset(PCollection<T> pCollection);

    <T> void putDataset(
        PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset, boolean cache);

    default <T> void putDataset(PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset) {
      putDataset(pCollection, dataset, true);
    }

    <T> Broadcast<SideInputValues<T>> getSideInputBroadcast(
        PCollection<T> pCollection, SideInputValues.Loader<T> loader);

    Supplier<PipelineOptions> getOptionsSupplier();

    PipelineOptions getOptions();

    SparkSession getSparkSession();
  }

  /**
   * {@link PTransformVisitor} that translates supported {@link PTransform PTransforms} into their
   * Spark correspondence.
   *
   * <p>Note, in some cases this involves the early evaluation of some parts of the pipeline. For
   * example, in order to use a side-input {@link org.apache.beam.sdk.values.PCollectionView
   * PCollectionView} in a translation the corresponding Spark {@link
   * org.apache.beam.runners.spark.translation.Dataset Dataset} might have to be collected and
   * broadcasted.
   */
  private class TranslatingVisitor extends PTransformVisitor implements TranslationState {
    private final Map<PCollection<?>, TranslationResult<?>> translationResults;
    private final Map<Coder<?>, Encoder<?>> encoders;
    private final SparkSession sparkSession;
    private final PipelineOptions options;
    private final Supplier<PipelineOptions> optionsSupplier;
    private final StorageLevel storageLevel;

    private final Set<TranslationResult<?>> leaves;

    public TranslatingVisitor(
        SparkSession sparkSession,
        SparkCommonPipelineOptions options,
        Map<PCollection<?>, TranslationResult<?>> translationResults) {
      this.sparkSession = sparkSession;
      this.translationResults = translationResults;
      this.options = options;
      this.optionsSupplier = new BroadcastOptions(sparkSession, options);
      this.storageLevel = StorageLevel.fromString(options.getStorageLevel());
      this.encoders = new HashMap<>();
      this.leaves = new HashSet<>();
    }

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {

      AppliedPTransform<InT, OutT, PTransform<InT, OutT>> appliedTransform =
          (AppliedPTransform) node.toAppliedPTransform(getPipeline());
      try {
        LOG.info(
            "Translating {}: {}",
            node.isCompositeNode() ? "composite" : "primitive",
            node.getFullName());
        translator.translate(transform, appliedTransform, this);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <T> Encoder<T> encoderOf(Coder<T> coder, Factory<T> factory) {
      // computeIfAbsent fails with Java 11 on recursive factory
      Encoder<T> enc = (Encoder<T>) encoders.get(coder);
      if (enc == null) {
        enc = factory.apply(coder);
        encoders.put(coder, enc);
      }
      return enc;
    }

    private <T> TranslationResult<T> getResult(PCollection<T> pCollection) {
      return (TranslationResult<T>) checkStateNotNull(translationResults.get(pCollection));
    }

    @Override
    public <T> Dataset<WindowedValue<T>> getDataset(PCollection<T> pCollection) {
      return checkStateNotNull(getResult(pCollection).dataset);
    }

    @Override
    public <T> void putDataset(
        PCollection<T> pCollection, Dataset<WindowedValue<T>> dataset, boolean cache) {
      TranslationResult<T> result = getResult(pCollection);
      if (cache && result.dependentTransforms.size() > 1) {
        LOG.info("Dataset {} will be cached.", result.name);
        result.dataset = dataset.persist(storageLevel); // use NONE to disable
      } else {
        result.dataset = dataset;
        if (result.dependentTransforms.isEmpty()) {
          leaves.add(result);
        }
      }
    }

    @Override
    public <T> Broadcast<SideInputValues<T>> getSideInputBroadcast(
        PCollection<T> pCollection, SideInputValues.Loader<T> loader) {
      TranslationResult<T> result = getResult(pCollection);
      if (result.sideInputBroadcast == null) {
        SideInputValues<T> sideInputValues = loader.apply(checkStateNotNull(result.dataset));
        result.sideInputBroadcast = broadcast(sparkSession, sideInputValues);
      }
      return result.sideInputBroadcast;
    }

    @Override
    public Supplier<PipelineOptions> getOptionsSupplier() {
      return optionsSupplier;
    }

    @Override
    public PipelineOptions getOptions() {
      return options;
    }

    @Override
    public SparkSession getSparkSession() {
      return sparkSession;
    }
  }

  /**
   * Supplier wrapping broadcasted {@link PipelineOptions} to avoid repeatedly serializing those as
   * part of the task closures.
   */
  private static class BroadcastOptions implements Supplier<PipelineOptions>, Serializable {
    private final Broadcast<SerializablePipelineOptions> broadcast;

    private BroadcastOptions(SparkSession session, PipelineOptions options) {
      this.broadcast = broadcast(session, new SerializablePipelineOptions(options));
    }

    @Override
    public PipelineOptions get() {
      return broadcast.value().get();
    }
  }

  private static <T> Broadcast<T> broadcast(SparkSession session, T t) {
    return session.sparkContext().broadcast(t, (ClassTag) ClassTag.AnyRef());
  }

  /**
   * {@link PTransformVisitor} that analyses dependencies of supported {@link PTransform
   * PTransforms} to help identify cache candidates.
   *
   * <p>The visitor may throw if a {@link PTransform} is observed that uses unsupported features.
   */
  private class DependencyVisitor extends PTransformVisitor {
    private final Map<PCollection<?>, TranslationResult<?>> results = new HashMap<>();

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {
      // add new translation result for every output of `transform`
      for (PCollection<?> pOut : node.getOutputs().values()) {
        results.put(pOut, new TranslationResult<>(pOut));
      }
      // track `transform` as downstream dependency for every input
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : node.getInputs().entrySet()) {
        TranslationResult<?> input = checkStateNotNull(results.get(entry.getValue()));
        input.dependentTransforms.add(transform);
      }
    }
  }

  /**
   * An abstract {@link PipelineVisitor} that visits all translatable {@link PTransform} pipeline
   * nodes of a pipeline with the respective {@link TransformTranslator}.
   *
   * <p>The visitor may throw if a {@link PTransform} is observed that uses unsupported features.
   */
  private abstract class PTransformVisitor extends PipelineVisitor.Defaults {

    /** Visit the {@link PTransform} with its respective {@link TransformTranslator}. */
    abstract <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator);

    @Override
    public final CompositeBehavior enterCompositeTransform(Node node) {
      PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getSupportedTranslator(transform);
      if (transform != null && translator != null) {
        visit(node, transform, translator);
        return DO_NOT_ENTER_TRANSFORM;
      } else {
        return ENTER_TRANSFORM;
      }
    }

    @Override
    public final void visitPrimitiveTransform(Node node) {
      PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
      if (transform == null || transform.getClass().equals(View.CreatePCollectionView.class)) {
        return; // ignore, nothing to be translated here, views are handled on the consumer side
      }
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getSupportedTranslator(transform);
      if (translator == null) {
        String urn = PTransformTranslation.urnForTransform(transform);
        throw new UnsupportedOperationException("Transform " + urn + " is not supported.");
      }
      visit(node, transform, translator);
    }

    /** {@link TransformTranslator} for {@link PTransform} if translation is known and supported. */
    private @Nullable TransformTranslator<PInput, POutput, PTransform<PInput, POutput>>
        getSupportedTranslator(@Nullable PTransform<PInput, POutput> transform) {
      if (transform == null) {
        return null;
      }
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getTransformTranslator(transform);
      return translator != null && translator.canTranslate(transform) ? translator : null;
    }
  }

  /**
   * Traverse the pipeline to check for unbounded {@link PCollection PCollections} that would
   * require streaming mode unless streaming mode is already enabled.
   */
  private static class StreamingModeDetector extends PipelineVisitor.Defaults {
    private boolean streaming;

    StreamingModeDetector(boolean streaming) {
      this.streaming = streaming;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      return streaming ? DO_NOT_ENTER_TRANSFORM : ENTER_TRANSFORM; // stop if in streaming mode
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (value instanceof PCollection && ((PCollection) value).isBounded() == UNBOUNDED) {
        LOG.info("Found unbounded PCollection {}, switching to streaming mode.", value.getName());
        streaming = true;
      }
    }
  }
}
