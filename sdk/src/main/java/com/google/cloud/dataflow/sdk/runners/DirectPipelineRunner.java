/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.dataflow.MapAggregatorValues;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Executes the operations in the pipeline directly, in this process, without
 * any optimization.  Useful for small local execution and tests.
 *
 * <p> Throws an exception from {@link #run} if execution fails.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DirectPipelineRunner
    extends PipelineRunner<DirectPipelineRunner.EvaluationResults> {
  private static final Logger LOG = LoggerFactory.getLogger(DirectPipelineRunner.class);

  /**
   * A map from PTransform class to the corresponding
   * TransformEvaluator to use to evaluate that transform.
   *
   * <p> A static map that contains system-wide defaults.
   */
  private static Map<Class, TransformEvaluator> defaultTransformEvaluators =
      new HashMap<>();

  /**
   * A map from PTransform class to the corresponding
   * TransformEvaluator to use to evaluate that transform.
   *
   * <p> An instance map that contains bindings for this DirectPipelineRunner.
   * Bindings in this map override those in the default map.
   */
  private Map<Class, TransformEvaluator> localTransformEvaluators =
      new HashMap<>();

  /**
   * Records that instances of the specified PTransform class
   * should be evaluated by default by the corresponding
   * TransformEvaluator.
   */
  public static <TransformT extends PTransform<?, ?>>
  void registerDefaultTransformEvaluator(
      Class<TransformT> transformClass,
      TransformEvaluator<TransformT> transformEvaluator) {
    if (defaultTransformEvaluators.put(transformClass, transformEvaluator)
        != null) {
      throw new IllegalArgumentException(
          "defining multiple evaluators for " + transformClass);
    }
  }

  /**
   * Records that instances of the specified PTransform class
   * should be evaluated by the corresponding TransformEvaluator.
   * Overrides any bindings specified by
   * {@link #registerDefaultTransformEvaluator}.
   */
  public <TransformT extends PTransform<?, ?>>
  void registerTransformEvaluator(
      Class<TransformT> transformClass,
      TransformEvaluator<TransformT> transformEvaluator) {
    if (localTransformEvaluators.put(transformClass, transformEvaluator)
        != null) {
      throw new IllegalArgumentException(
          "defining multiple evaluators for " + transformClass);
    }
  }

  /**
   * Returns the TransformEvaluator to use for instances of the
   * specified PTransform class, or null if none registered.
   */
  public <TransformT extends PTransform<?, ?>>
      TransformEvaluator<TransformT> getTransformEvaluator(Class<TransformT> transformClass) {
    TransformEvaluator<TransformT> transformEvaluator =
        localTransformEvaluators.get(transformClass);
    if (transformEvaluator == null) {
      transformEvaluator = defaultTransformEvaluators.get(transformClass);
    }
    return transformEvaluator;
  }

  /**
   * Constructs a DirectPipelineRunner from the given options.
   */
  public static DirectPipelineRunner fromOptions(PipelineOptions options) {
    DirectPipelineOptions directOptions =
        PipelineOptionsValidator.validate(DirectPipelineOptions.class, options);
    LOG.debug("Creating DirectPipelineRunner");
    return new DirectPipelineRunner(directOptions);
  }

  /**
   * Constructs a runner with default properties for testing.
   *
   * @return The newly created runner.
   */
  public static DirectPipelineRunner createForTest() {
    DirectPipelineOptions options = PipelineOptionsFactory.as(DirectPipelineOptions.class);
    options.setStableUniqueNames(CheckEnabled.ERROR);
    options.setGcpCredential(new TestCredential());
    return new DirectPipelineRunner(options);
  }

  /**
   * Enable runtime testing to verify that all functions and {@link Coder}
   * instances can be serialized.
   *
   * <p> Enabled by default.
   *
   * <p> This method modifies the {@code DirectPipelineRunner} instance and
   * returns itself.
   */
  public DirectPipelineRunner withSerializabilityTesting(boolean enable) {
    this.testSerializability = enable;
    return this;
  }

  /**
   * Enable runtime testing to verify that all values can be encoded.
   *
   * <p> Enabled by default.
   *
   * <p> This method modifies the {@code DirectPipelineRunner} instance and
   * returns itself.
   */
  public DirectPipelineRunner withEncodabilityTesting(boolean enable) {
    this.testEncodability = enable;
    return this;
  }

  /**
   * Enable runtime testing to verify that functions do not depend on order
   * of the elements.
   *
   * <p> This is accomplished by randomizing the order of elements.
   *
   * <p> Enabled by default.
   *
   * <p> This method modifies the {@code DirectPipelineRunner} instance and
   * returns itself.
   */
  public DirectPipelineRunner withUnorderednessTesting(boolean enable) {
    this.testUnorderedness = enable;
    return this;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (transform instanceof Combine.GroupedValues) {
      return (OutputT) applyTestCombine((Combine.GroupedValues) transform, (PCollection) input);
    } else {
      return super.apply(transform, input);
    }
  }

  private <K, InputT, AccumT, OutputT> PCollection<KV<K, OutputT>> applyTestCombine(
      Combine.GroupedValues<K, InputT, OutputT> transform,
      PCollection<KV<K, Iterable<InputT>>> input) {

    PCollection<KV<K, OutputT>> output = input
        .apply(ParDo.of(TestCombineDoFn.create(transform, input, testSerializability)));

    try {
      output.setCoder(transform.getDefaultOutputCoder(input));
    } catch (CannotProvideCoderException exc) {
      // let coder inference occur later, if it can
    }
    return output;
  }

  /**
   * The implementation may split the {@link KeyedCombineFn} into ADD, MERGE
   * and EXTRACT phases (see {@code com.google.cloud.dataflow.sdk.runners.worker.CombineValuesFn}).
   * In order to emulate
   * this for the {@link DirectPipelineRunner} and provide an experience
   * closer to the service, go through heavy seralizability checks for
   * the equivalent of the results of the ADD phase, but after the
   * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey}
   * shuffle, and the MERGE phase. Doing these checks
   * ensure that not only is the accumulator coder serializable, but
   * the accumulator coder can actually serialize the data in
   * question.
   */
  // @VisibleForTesting
  @SuppressWarnings("serial")
  public static class TestCombineDoFn<K, InputT, AccumT, OutputT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>> {
    private final KeyedCombineFn<? super K, ? super InputT, AccumT, OutputT> fn;
    private final Coder<AccumT> accumCoder;
    private final boolean testSerializability;

    public static <K, InputT, AccumT, OutputT> TestCombineDoFn<K, InputT, AccumT, OutputT> create(
        Combine.GroupedValues<K, InputT, OutputT> transform,
        PCollection<KV<K, Iterable<InputT>>> input,
        boolean testSerializability) {

      Coder<AccumT> accumCoder;
      try {
        accumCoder = (Coder<AccumT>) transform.getAccumulatorCoder(
            input.getPipeline().getCoderRegistry(), input);
      } catch (CannotProvideCoderException exc) {
        throw new IllegalArgumentException(
          "Transform " + transform + " failed to provide a coder for its accumulator type");
      }

      return new TestCombineDoFn(
          transform.getFn(),
          accumCoder,
          testSerializability);
    }

    public TestCombineDoFn(
        KeyedCombineFn<? super K, ? super InputT, AccumT, OutputT> fn,
        Coder<AccumT> accumCoder,
        boolean testSerializability) {
      this.fn = fn;
      this.accumCoder = accumCoder;
      this.testSerializability = testSerializability;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      K key = c.element().getKey();
      Iterable<InputT> values = c.element().getValue();
      List<AccumT> groupedPostShuffle =
          ensureSerializableByCoder(ListCoder.of(accumCoder),
              addInputsRandomly(fn, key, values, new Random()),
              "After addInputs of KeyedCombineFn " + fn.toString());
      AccumT merged =
          ensureSerializableByCoder(accumCoder,
              fn.mergeAccumulators(key, groupedPostShuffle),
              "After mergeAccumulators of KeyedCombineFn " + fn.toString());
      // Note: The serializability of KV<K, OutputT> is ensured by the
      // runner itself, since it's a transform output.
      c.output(KV.of(key, fn.extractOutput(key, merged)));
    }

    // Create a random list of accumulators from the given list of values
    // @VisibleForTesting
    public static <K, AccumT, InputT> List<AccumT> addInputsRandomly(
        KeyedCombineFn<? super K, ? super InputT, AccumT, ?> fn,
        K key,
        Iterable<InputT> values,
        Random random) {
      List<AccumT> out = new ArrayList<AccumT>();
      int i = 0;
      AccumT accumulator = fn.createAccumulator(key);
      boolean hasInput = false;

      for (InputT value : values) {
        accumulator = fn.addInput(key, accumulator, value);
        hasInput = true;

        // For each index i, flip a 1/2^i weighted coin for whether to
        // create a new accumulator after index i is added, i.e. [0]
        // is guaranteed, [1] is an even 1/2, [2] is 1/4, etc. The
        // goal is to partition the inputs into accumulators, and make
        // the accumulators potentially lumpy.
        if (i == 0 || random.nextInt(1 << Math.min(i, 30)) == 0) {
          out.add(accumulator);
          accumulator = fn.createAccumulator(key);
          hasInput = false;
        }
        i++;
      }
      if (hasInput) {
        out.add(accumulator);
      }

      Collections.shuffle(out, random);
      return out;
    }

    public <T> T ensureSerializableByCoder(
        Coder<T> coder, T value, String errorContext) {
      if (testSerializability) {
        return SerializableUtils.ensureSerializableByCoder(
            coder, value, errorContext);
      }
      return value;
    }
  }

  @Override
  public EvaluationResults run(Pipeline pipeline) {
    LOG.info("Executing pipeline using the DirectPipelineRunner.");

    Evaluator evaluator = new Evaluator();
    evaluator.run(pipeline);

    // Log all counter values for debugging purposes.
    for (Counter counter : evaluator.getCounters()) {
      LOG.info("Final aggregator value: {}", counter);
    }

    LOG.info("Pipeline execution complete.");

    return evaluator;
  }

  /**
   * An evaluator of a PTransform.
   */
  public interface TransformEvaluator<TransformT extends PTransform> {
    public void evaluate(TransformT transform,
                         EvaluationContext context);
  }

  /**
   * The interface provided to registered callbacks for interacting
   * with the {@code DirectPipelineRunner}, including reading and writing the
   * values of {@link PCollection}s and {@link PCollectionView}s.
   */
  public interface EvaluationResults extends PipelineResult {
    /**
     * Retrieves the value of the given PCollection.
     * Throws an exception if the PCollection's value hasn't already been set.
     */
    <T> List<T> getPCollection(PCollection<T> pc);

    /**
     * Retrieves the windowed value of the given PCollection.
     * Throws an exception if the PCollection's value hasn't already been set.
     */
    <T> List<WindowedValue<T>> getPCollectionWindowedValues(PCollection<T> pc);

    /**
     * Retrieves the values of each PCollection in the given
     * PCollectionList. Throws an exception if the PCollectionList's
     * value hasn't already been set.
     */
    <T> List<List<T>> getPCollectionList(PCollectionList<T> pcs);

    /**
     * Retrieves the values indicated by the given {@link PCollectionView}.
     * Note that within the {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context}
     * implementation a {@link PCollectionView} should convert from this representation to a
     * suitable side input value.
     */
    <T, WindowedT> Iterable<WindowedValue<?>> getPCollectionView(PCollectionView<T> view);
  }

  /**
   * An immutable (value, timestamp) pair, along with other metadata necessary
   * for the implementation of {@code DirectPipelineRunner}.
   */
  public static class ValueWithMetadata<V> {
    /**
     * Returns a new {@code ValueWithMetadata} with the {@code WindowedValue}.
     * Key is null.
     */
    public static <V> ValueWithMetadata<V> of(WindowedValue<V> windowedValue) {
      return new ValueWithMetadata<>(windowedValue, null);
    }

    /**
     * Returns a new {@code ValueWithMetadata} with the implicit key associated
     * with this value set.  The key is the last key grouped by in the chain of
     * productions that produced this element.
     * These keys are used internally by {@link DirectPipelineRunner} for keeping
     * {@link com.google.cloud.dataflow.sdk.util.WindowingInternals.KeyedState} separate
     * across keys.
     */
    public ValueWithMetadata<V> withKey(Object key) {
      return new ValueWithMetadata<>(windowedValue, key);
    }

    /**
     * Returns a new {@code ValueWithMetadata} that is a copy of this one, but with
     * a different value.
     */
    public <T> ValueWithMetadata<T> withValue(T value) {
      return new ValueWithMetadata(windowedValue.withValue(value), getKey());
    }

    /**
     * Returns the {@code WindowedValue} associated with this element.
     */
    public WindowedValue<V> getWindowedValue() {
      return windowedValue;
    }

    /**
     * Returns the value associated with this element.
     *
     * @see #withValue
     */
    public V getValue() {
      return windowedValue.getValue();
    }

    /**
     * Returns the timestamp associated with this element.
     */
    public Instant getTimestamp() {
      return windowedValue.getTimestamp();
    }

    /**
     * Returns the collection of windows this element has been placed into.  May
     * be null if the {@code PCollection} this element is in has not yet been
     * windowed.
     *
     * @see #getWindows()
     */
    public Collection<? extends BoundedWindow> getWindows() {
      return windowedValue.getWindows();
    }


    /**
     * Returns the key associated with this element.  May be null if the
     * {@code PCollection} this element is in is not keyed.
     *
     * @see #withKey
     */
    public Object getKey() {
      return key;
    }

    ////////////////////////////////////////////////////////////////////////////

  private final Object key;
    private final WindowedValue<V> windowedValue;

    private ValueWithMetadata(WindowedValue<V> windowedValue,
                              Object key) {
      this.windowedValue = windowedValue;
      this.key = key;
    }
  }

  /**
   * The interface provided to registered callbacks for interacting
   * with the {@code DirectPipelineRunner}, including reading and writing the
   * values of {@link PCollection}s and {@link PCollectionView}s.
   */
  public interface EvaluationContext extends EvaluationResults {
    /**
     * Returns the configured pipeline options.
     */
    DirectPipelineOptions getPipelineOptions();

    /**
     * Returns the input of the currently being processed transform.
     */
    <InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform);

    /**
     * Returns the output of the currently being processed transform.
     */
    <OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform);

    /**
     * Sets the value of the given PCollection, where each element also has a timestamp
     * and collection of windows.
     * Throws an exception if the PCollection's value has already been set.
     */
    <T> void setPCollectionValuesWithMetadata(
        PCollection<T> pc, List<ValueWithMetadata<T>> elements);

    /**
     * Sets the value of the given PCollection, where each element also has a timestamp
     * and collection of windows.
     * Throws an exception if the PCollection's value has already been set.
     */
    <T> void setPCollectionWindowedValue(PCollection<T> pc, List<WindowedValue<T>> elements);

    /**
     * Shorthand for setting the value of a PCollection where the elements do not have
     * timestamps or windows.
     * Throws an exception if the PCollection's value has already been set.
     */
    <T> void setPCollection(PCollection<T> pc, List<T> elements);

    /**
     * Retrieves the value of the given PCollection, along with element metadata
     * such as timestamps and windows.
     * Throws an exception if the PCollection's value hasn't already been set.
     */
    <T> List<ValueWithMetadata<T>> getPCollectionValuesWithMetadata(PCollection<T> pc);

    /**
     * Sets the value associated with the given {@link PCollectionView}.
     * Throws an exception if the {@link PCollectionView}'s value has already been set.
     */
    <ElemT, T, WindowedT> void setPCollectionView(
        PCollectionView<T> pc,
        Iterable<WindowedValue<ElemT>> value);

    /**
     * Ensures that the element is encodable and decodable using the
     * TypePValue's coder, by encoding it and decoding it, and
     * returning the result.
     */
    <T> T ensureElementEncodable(TypedPValue<T> pvalue, T element);

    /**
     * If the evaluation context is testing unorderedness,
     * randomly permutes the order of the elements, in a
     * copy if !inPlaceAllowed, and returns the permuted list,
     * otherwise returns the argument unchanged.
     */
    <T> List<T> randomizeIfUnordered(List<T> elements,
                                     boolean inPlaceAllowed);

    /**
     * If the evaluation context is testing serializability, ensures
     * that the argument function is serializable and deserializable
     * by encoding it and then decoding it, and returning the result.
     * Otherwise returns the argument unchanged.
     */
    <FunctionT extends Serializable> FunctionT ensureSerializable(FunctionT fn);

    /**
     * If the evaluation context is testing serializability, ensures
     * that the argument Coder is serializable and deserializable
     * by encoding it and then decoding it, and returning the result.
     * Otherwise returns the argument unchanged.
     */
    <T> Coder<T> ensureCoderSerializable(Coder<T> coder);

    /**
     * If the evaluation context is testing serializability, ensures
     * that the given data is serializable and deserializable with the
     * given Coder by encoding it and then decoding it, and returning
     * the result. Otherwise returns the argument unchanged.
     *
     * <p> Error context is prefixed to any thrown exceptions.
     */
    <T> T ensureSerializableByCoder(Coder<T> coder,
                                    T data, String errorContext);

    /**
     * Returns a mutator, which can be used to add additional counters to
     * this EvaluationContext.
     */
    CounterSet.AddCounterMutator getAddCounterMutator();

    /**
     * Gets the step name for this transform.
     */
    public String getStepName(PTransform<?, ?> transform);
  }


  /////////////////////////////////////////////////////////////////////////////

  class Evaluator implements PipelineVisitor, EvaluationContext {
    /**
     * A map from PTransform to the step name of that transform. This is the internal name for the
     * transform (e.g. "s2").
     */
    private final Map<PTransform<?, ?>, String> stepNames = new HashMap<>();
    private final Map<PValue, Object> store = new HashMap<>();
    private final CounterSet counters = new CounterSet();
    private AppliedPTransform<?, ?, ?> currentTransform;

    private Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps = null;

    /**
     * A map from PTransform to the full name of that transform. This is the user name of the
     * transform (e.g. "RemoveDuplicates/Combine/GroupByKey").
     */
    private final Map<PTransform<?, ?>, String> fullNames = new HashMap<>();

    // Use a random number generator with a fixed seed, so execution
    // using this evaluator is deterministic.  (If the user-defined
    // functions, transforms, and coders are deterministic.)
    Random rand = new Random(0);

    public Evaluator() {}

    public void run(Pipeline pipeline) {
      pipeline.traverseTopologically(this);
      aggregatorSteps = new AggregatorPipelineExtractor(pipeline).getAggregatorSteps();
    }

    @Override
    public DirectPipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public <InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform) {
      checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
          "can only be called with current transform");
      return (InputT) currentTransform.getInput();
    }

    @Override
    public <OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform) {
      checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
          "can only be called with current transform");
      return (OutputT) currentTransform.getOutput();
    }

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
      PTransform<?, ?> transform = node.getTransform();
      fullNames.put(transform, node.getFullName());
      TransformEvaluator evaluator =
          getTransformEvaluator(transform.getClass());
      if (evaluator == null) {
        throw new IllegalStateException(
            "no evaluator registered for " + transform);
      }
      LOG.debug("Evaluating {}", transform);
      currentTransform = AppliedPTransform.of(
          node.getFullName(), node.getInput(), node.getOutput(), (PTransform) transform);
      evaluator.evaluate(transform, this);
      currentTransform = null;
    }

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {
      LOG.debug("Checking evaluation of {}", value);
      if (value.getProducingTransformInternal() == null) {
        throw new RuntimeException(
            "internal error: expecting a PValue " +
            "to have a producingTransform");
      }
      if (!producer.isCompositeNode()) {
        // Verify that primitive transform outputs are already computed.
        getPValue(value);
      }
    }

    /**
     * Sets the value of the given PValue.
     * Throws an exception if the PValue's value has already been set.
     */
    void setPValue(PValue pvalue, Object contents) {
      if (store.containsKey(pvalue)) {
        throw new IllegalStateException(
            "internal error: setting the value of " + pvalue +
            " more than once");
      }
      store.put(pvalue, contents);
    }

    /**
     * Retrieves the value of the given PValue.
     * Throws an exception if the PValue's value hasn't already been set.
     */
    Object getPValue(PValue pvalue) {
      if (!store.containsKey(pvalue)) {
        throw new IllegalStateException(
            "internal error: getting the value of " + pvalue +
            " before it has been computed");
      }
      return store.get(pvalue);
    }

    /**
     * Convert a list of T to a list of {@code ValueWithMetadata<T>}, with a timestamp of 0
     * and null windows.
     */
    <T> List<ValueWithMetadata<T>> toValueWithMetadata(List<T> values) {
      List<ValueWithMetadata<T>> result = new ArrayList<>(values.size());
      for (T value : values) {
        result.add(ValueWithMetadata.of(WindowedValue.valueInGlobalWindow(value)));
      }
      return result;
    }

    /**
     * Convert a list of {@code WindowedValue<T>} to a list of {@code ValueWithMetadata<T>}.
     */
    <T> List<ValueWithMetadata<T>> toValueWithMetadataFromWindowedValue(
        List<WindowedValue<T>> values) {
      List<ValueWithMetadata<T>> result = new ArrayList<>(values.size());
      for (WindowedValue<T> value : values) {
        result.add(ValueWithMetadata.of(value));
      }
      return result;
    }

    @Override
    public <T> void setPCollection(PCollection<T> pc, List<T> elements) {
      setPCollectionValuesWithMetadata(pc, toValueWithMetadata(elements));
    }

    @Override
    public <T> void setPCollectionWindowedValue(
        PCollection<T> pc, List<WindowedValue<T>> elements) {
      setPCollectionValuesWithMetadata(pc, toValueWithMetadataFromWindowedValue(elements));
    }

    @Override
    public <T> void setPCollectionValuesWithMetadata(
        PCollection<T> pc, List<ValueWithMetadata<T>> elements) {
      LOG.debug("Setting {} = {}", pc, elements);
      setPValue(pc, ensurePCollectionEncodable(pc, elements));
    }

    @Override
    public <ElemT, T, WindowedT> void setPCollectionView(
        PCollectionView<T> view,
        Iterable<WindowedValue<ElemT>> value) {
      LOG.debug("Setting {} = {}", view, value);
      setPValue(view, value);
    }

    /**
     * Retrieves the value of the given PCollection.
     * Throws an exception if the PCollection's value hasn't already been set.
     */
    @Override
    public <T> List<T> getPCollection(PCollection<T> pc) {
      List<T> result = new ArrayList<>();
      for (ValueWithMetadata<T> elem : getPCollectionValuesWithMetadata(pc)) {
        result.add(elem.getValue());
      }
      return result;
    }

    @Override
    public <T> List<WindowedValue<T>> getPCollectionWindowedValues(PCollection<T> pc) {
      return Lists.transform(
          getPCollectionValuesWithMetadata(pc),
          new Function<ValueWithMetadata<T>, WindowedValue<T>>() {
            @Override
            public WindowedValue<T> apply(ValueWithMetadata<T> input) {
              return input.getWindowedValue();
            }});
    }

    @Override
    public <T> List<ValueWithMetadata<T>> getPCollectionValuesWithMetadata(PCollection<T> pc) {
      List<ValueWithMetadata<T>> elements = (List<ValueWithMetadata<T>>) getPValue(pc);
      elements = randomizeIfUnordered(elements, false /* not inPlaceAllowed */);
      LOG.debug("Getting {} = {}", pc, elements);
      return elements;
    }

    @Override
    public <T> List<List<T>> getPCollectionList(PCollectionList<T> pcs) {
      List<List<T>> elementsList = new ArrayList<>();
      for (PCollection<T> pc : pcs.getAll()) {
        elementsList.add(getPCollection(pc));
      }
      return elementsList;
    }

    /**
     * Retrieves the value indicated by the given {@link PCollectionView}.
     * Note that within the {@link DoFnContext} a {@link PCollectionView}
     * converts from this representation to a suitable side input value.
     */
    @Override
    public <T, WindowedT> Iterable<WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
      Iterable<WindowedValue<?>> value = (Iterable<WindowedValue<?>>) getPValue(view);
      LOG.debug("Getting {} = {}", view, value);
      return value;
    }

    /**
     * If testEncodability, ensures that the PCollection's coder and elements
     * are encodable and decodable by encoding them and decoding them,
     * and returning the result.  Otherwise returns the argument elements.
     */
    <T> List<ValueWithMetadata<T>> ensurePCollectionEncodable(
        PCollection<T> pc, List<ValueWithMetadata<T>> elements) {
      ensureCoderSerializable(pc.getCoder());
      if (!testEncodability) {
        return elements;
      }
      List<ValueWithMetadata<T>> elementsCopy = new ArrayList<>(elements.size());
      for (ValueWithMetadata<T> element : elements) {
        elementsCopy.add(
            element.withValue(ensureElementEncodable(pc, element.getValue())));
      }
      return elementsCopy;
    }

    @Override
    public <T> T ensureElementEncodable(TypedPValue<T> pvalue, T element) {
      return ensureSerializableByCoder(
          pvalue.getCoder(), element, "Within " + pvalue.toString());
    }

    @Override
    public <T> List<T> randomizeIfUnordered(List<T> elements,
                                            boolean inPlaceAllowed) {
      if (!testUnorderedness) {
        return elements;
      }
      List<T> elementsCopy = new ArrayList<>(elements);
      Collections.shuffle(elementsCopy, rand);
      return elementsCopy;
    }

    @Override
    public <FunctionT extends Serializable> FunctionT ensureSerializable(FunctionT fn) {
      if (!testSerializability) {
        return fn;
      }
      return SerializableUtils.ensureSerializable(fn);
    }

    @Override
    public <T> Coder<T> ensureCoderSerializable(Coder<T> coder) {
      if (testSerializability) {
        SerializableUtils.ensureSerializable(coder);
      }
      return coder;
    }

    @Override
    public <T> T ensureSerializableByCoder(
        Coder<T> coder, T value, String errorContext) {
      if (testSerializability) {
        return SerializableUtils.ensureSerializableByCoder(
            coder, value, errorContext);
      }
      return value;
    }

    @Override
    public CounterSet.AddCounterMutator getAddCounterMutator() {
      return counters.getAddCounterMutator();
    }

    @Override
    public String getStepName(PTransform<?, ?> transform) {
      String stepName = stepNames.get(transform);
      if (stepName == null) {
        stepName = "s" + (stepNames.size() + 1);
        stepNames.put(transform, stepName);
      }
      return stepName;
    }

    /**
     * Returns the CounterSet generated during evaluation, which includes
     * user-defined Aggregators and may include system-defined counters.
     */
    public CounterSet getCounters() {
      return counters;
    }

    /**
     * Returns JobState.DONE in all situations. The Evaluator is not returned
     * until the pipeline has been traversed, so it will either be returned
     * after a successful run or the run call will terminate abnormally.
     */
    @Override
    public State getState() {
      return State.DONE;
    }

    @Override
    public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) {
      Map<String, T> stepValues = new HashMap<>();
      for (PTransform<?, ?> step : aggregatorSteps.get(aggregator)) {
        String stepName = String.format("user-%s-%s", stepNames.get(step), aggregator.getName());
        String fullName = fullNames.get(step);
        Counter<?> counter = counters.getExistingCounter(stepName);
        if (counter == null) {
          throw new IllegalArgumentException(
              "Aggregator " + aggregator + " is not used in this pipeline");
        }
        stepValues.put(fullName, (T) counter.getAggregate());
      }
      return new MapAggregatorValues<>(stepValues);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private final DirectPipelineOptions options;
  private boolean testSerializability = true;
  private boolean testEncodability = true;
  private boolean testUnorderedness = true;

  /** Returns a new DirectPipelineRunner. */
  private DirectPipelineRunner(DirectPipelineOptions options) {
    this.options = options;
    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerStandardIOFactories(options);
  }

  public DirectPipelineOptions getPipelineOptions() {
    return options;
  }

  @Override
  public String toString() {
    return "DirectPipelineRunner#" + hashCode();
  }
}
