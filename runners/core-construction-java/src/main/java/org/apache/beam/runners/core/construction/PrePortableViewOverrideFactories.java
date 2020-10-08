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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link PTransformOverrideFactory PTransformOverrideFactories} that replace the expansions of
 * {@link View} transforms with their pre-portability forms, to allow the core SDK to have a single
 * portable expansion that matches the Beam model's architecture for side inputs.
 */
public class PrePortableViewOverrideFactories<T> {
  private PrePortableViewOverrideFactories() {}

  public static class ViewAsListOverrideFactory<T>
      extends SingleInputOutputOverrideFactory<
          PCollection<T>, PCollectionView<List<T>>, View.AsList<T>> {
    @Override
    public PTransformReplacement<PCollection<T>, PCollectionView<List<T>>> getReplacementTransform(
        AppliedPTransform<PCollection<T>, PCollectionView<List<T>>, View.AsList<T>> original) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(original),
          new PrePortableAsList<>(findPCollectionView(original)));
    }
  }

  public static class ViewAsIterableOverrideFactory<T>
      extends SingleInputOutputOverrideFactory<
          PCollection<T>,
          PCollectionView<Iterable<T>>,
          PTransform<PCollection<T>, PCollectionView<Iterable<T>>>> {
    @Override
    public PTransformReplacement<PCollection<T>, PCollectionView<Iterable<T>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<T>,
                    PCollectionView<Iterable<T>>,
                    PTransform<PCollection<T>, PCollectionView<Iterable<T>>>>
                original) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(original),
          new PrePortableAsIterable<>(findPCollectionView(original)));
    }
  }

  public static class ViewAsMapOverrideFactory<K, V>
      extends SingleInputOutputOverrideFactory<
          PCollection<KV<K, V>>,
          PCollectionView<Map<K, V>>,
          PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>>> {
    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>,
                    PCollectionView<Map<K, V>>,
                    PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>>>
                original) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(original),
          new PrePortableAsMap<>(findPCollectionView(original)));
    }
  }

  public static class ViewAsMultiMapOverrideFactory<K, V>
      extends SingleInputOutputOverrideFactory<
          PCollection<KV<K, V>>,
          PCollectionView<Map<K, Iterable<V>>>,
          PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>>> {
    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>,
                    PCollectionView<Map<K, Iterable<V>>>,
                    PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>>>
                original) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(original),
          new PrePortableAsMultimap<>(findPCollectionView(original)));
    }
  }

  public static class CombineAsSingletonViewOverrideFactory<InputT, OutputT>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>,
          PCollectionView<OutputT>,
          Combine.GloballyAsSingletonView<InputT, OutputT>> {
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionView<OutputT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<InputT>,
                    PCollectionView<OutputT>,
                    Combine.GloballyAsSingletonView<InputT, OutputT>>
                original) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(original),
          new PrePortableCombineAsSingletonView<>(
              original.getTransform(), findPCollectionView(original)));
    }
  }

  @Internal
  public static class PrePortableAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    private final PCollectionView<List<T>> originalView;

    private PrePortableAsList(PCollectionView<List<T>> originalView) {
      this.originalView = originalView;
    }

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }

      PCollection<KV<Void, T>> materializationInput =
          input.apply(new VoidKeyToMultimapMaterialization<>());

      PCollectionView<List<T>> view =
          PCollectionViews.listViewUsingVoidKey(
              (TupleTag<Materializations.MultimapView<Void, T>>) originalView.getTagInternal(),
              materializationInput,
              input.getCoder()::getEncodedTypeDescriptor,
              materializationInput.getWindowingStrategy());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Public only so a {@link PipelineRunner} may override its behavior.
   *
   * <p>See {@link View#asMap()}.
   */
  @Internal
  public static class PrePortableAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {
    private final PCollectionView<Map<K, V>> originalView;

    private PrePortableAsMap(PCollectionView<Map<K, V>> originalView) {
      this.originalView = originalView;
    }

    /** @deprecated this method simply returns this AsMap unmodified */
    @Deprecated()
    public PrePortableAsMap<K, V> withSingletonValues() {
      return this;
    }

    @Override
    public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }

      KvCoder<K, V> kvCoder = (KvCoder<K, V>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<V> valueCoder = kvCoder.getValueCoder();

      PCollection<KV<Void, KV<K, V>>> materializationInput =
          input.apply(new VoidKeyToMultimapMaterialization<>());
      PCollectionView<Map<K, V>> view =
          PCollectionViews.mapViewUsingVoidKey(
              (TupleTag<Materializations.MultimapView<Void, KV<K, V>>>)
                  originalView.getTagInternal(),
              materializationInput,
              (PCollectionViews.TypeDescriptorSupplier<K>) keyCoder::getEncodedTypeDescriptor,
              (PCollectionViews.TypeDescriptorSupplier<V>) valueCoder::getEncodedTypeDescriptor,
              materializationInput.getWindowingStrategy());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Public only so a {@link PipelineRunner} may override its behavior.
   *
   * <p>See {@link View#asMultimap()}.
   */
  @Internal
  public static class PrePortableAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {
    private final PCollectionView<Map<K, Iterable<V>>> originalView;

    private PrePortableAsMultimap(PCollectionView<Map<K, Iterable<V>>> originalView) {
      this.originalView = originalView;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }

      KvCoder<K, V> kvCoder = (KvCoder<K, V>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<V> valueCoder = kvCoder.getValueCoder();
      PCollection<KV<Void, KV<K, V>>> materializationInput =
          input.apply(new VoidKeyToMultimapMaterialization<>());
      PCollectionView<Map<K, Iterable<V>>> view =
          PCollectionViews.multimapViewUsingVoidKey(
              (TupleTag<Materializations.MultimapView<Void, KV<K, V>>>)
                  originalView.getTagInternal(),
              materializationInput,
              (PCollectionViews.TypeDescriptorSupplier<K>) keyCoder::getEncodedTypeDescriptor,
              (PCollectionViews.TypeDescriptorSupplier<V>) valueCoder::getEncodedTypeDescriptor,
              materializationInput.getWindowingStrategy());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Public only so a {@link PipelineRunner} may override its behavior.
   *
   * <p>See {@link View#asIterable()}.
   */
  @Internal
  public static class PrePortableAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {
    private final PCollectionView<Iterable<T>> originalView;

    private PrePortableAsIterable(PCollectionView<Iterable<T>> originalView) {
      this.originalView = originalView;
    }

    @Override
    public PCollectionView<Iterable<T>> expand(PCollection<T> input) {
      try {
        GroupByKey.applicableTo(input);
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Unable to create a side-input view from input", e);
      }

      PCollection<KV<Void, T>> materializationInput =
          input.apply(new VoidKeyToMultimapMaterialization<>());
      Coder<T> inputCoder = input.getCoder();
      PCollectionView<Iterable<T>> view =
          PCollectionViews.iterableViewUsingVoidKey(
              (TupleTag<Materializations.MultimapView<Void, T>>) originalView.getTagInternal(),
              materializationInput,
              inputCoder::getEncodedTypeDescriptor,
              materializationInput.getWindowingStrategy());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }
  }

  /** Pre-portability override for {@code Combine.GloballyAsSingletonView<InputT, OutputT>}. */
  public static class PrePortableCombineAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

    private final Combine.GloballyAsSingletonView<InputT, OutputT> original;
    private final PCollectionView<OutputT> originalView;

    private PrePortableCombineAsSingletonView(
        Combine.GloballyAsSingletonView<InputT, OutputT> original,
        PCollectionView<OutputT> originalView) {
      this.original = original;
      this.originalView = originalView;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(
              Combine.<InputT, OutputT>globally(getCombineFn())
                  .withoutDefaults()
                  .withFanout(getFanout()));
      PCollection<KV<Void, OutputT>> materializationInput =
          combined.apply(new VoidKeyToMultimapMaterialization<>());
      Coder<OutputT> outputCoder = combined.getCoder();
      PCollectionView<OutputT> view =
          PCollectionViews.singletonViewUsingVoidKey(
              (TupleTag<Materializations.MultimapView<Void, OutputT>>)
                  originalView.getTagInternal(),
              materializationInput,
              outputCoder::getEncodedTypeDescriptor,
              input.getWindowingStrategy(),
              getInsertDefault(),
              getInsertDefault() ? getCombineFn().defaultValue() : null,
              combined.getCoder());
      materializationInput.apply(View.CreatePCollectionView.of(view));
      return view;
    }

    public int getFanout() {
      return original.getFanout();
    }

    public boolean getInsertDefault() {
      return original.getInsertDefault();
    }

    public CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> getCombineFn() {
      return original.getCombineFn();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      original.populateDisplayData(builder);
    }
  }

  /**
   * A {@link PTransform} which converts all values into {@link KV}s with {@link Void} keys.
   *
   * <p>TODO(BEAM-10097): Replace this materialization with specializations that optimize the
   * various SDK requested views.
   */
  @Internal
  static class VoidKeyToMultimapMaterialization<T>
      extends PTransform<PCollection<T>, PCollection<KV<Void, T>>> {

    private static class VoidKeyToMultimapMaterializationDoFn<T> extends DoFn<T, KV<Void, T>> {
      @ProcessElement
      public void processElement(@Element T element, OutputReceiver<KV<Void, T>> r) {
        r.output(KV.of((Void) null, element));
      }
    }

    @Override
    public PCollection<KV<Void, T>> expand(PCollection<T> input) {
      PCollection<KV<Void, T>> output =
          input.apply(ParDo.of(new VoidKeyToMultimapMaterializationDoFn<>()));
      output.setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()));
      return output;
    }
  }

  private static <InputT, ViewT> PCollectionView<ViewT> findPCollectionView(
      final AppliedPTransform<
              PCollection<InputT>,
              PCollectionView<ViewT>,
              ? extends PTransform<PCollection<InputT>, PCollectionView<ViewT>>>
          transform) {
    final AtomicReference<@Nullable PCollectionView<ViewT>> viewRef = new AtomicReference<>();
    transform
        .getPipeline()
        .traverseTopologically(
            new Pipeline.PipelineVisitor.Defaults() {
              // Stores whether we have entered the expected composite view transform.
              private boolean tracking = false;

              @Override
              public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
                if (transform.getTransform() == node.getTransform()) {
                  tracking = true;
                }
                return super.enterCompositeTransform(node);
              }

              @Override
              public void visitPrimitiveTransform(TransformHierarchy.Node node) {
                @Nullable PTransform<?, ?> primitiveTransform = node.getTransform();
                if (tracking && primitiveTransform instanceof View.CreatePCollectionView) {
                  View.CreatePCollectionView<InputT, ViewT> createViewTransform =
                      (View.CreatePCollectionView<InputT, ViewT>) primitiveTransform;
                  checkState(
                      viewRef.compareAndSet(null, createViewTransform.getView()),
                      "Found more than one instance of a CreatePCollectionView when"
                          + "attempting to replace %s, found [%s, %s]",
                      primitiveTransform,
                      viewRef.get(),
                      createViewTransform.getView());
                }
              }

              @Override
              public void leaveCompositeTransform(TransformHierarchy.Node node) {
                if (transform.getTransform() == node.getTransform()) {
                  tracking = false;
                }
              }
            });
    @Nullable PCollectionView<ViewT> view = viewRef.get();
    checkState(
        view != null,
        "Expected to find CreatePCollectionView contained within %s",
        transform.getTransform());
    assert view != null : "@AssumeAssertion(nullness): unreachable if null";
    return view;
  }
}
