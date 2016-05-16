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
package org.apache.beam.sdk.runners;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.DirectPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.AssignWindows;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.MapAggregatorValues;
import org.apache.beam.sdk.util.PerKeyCombineFnRunner;
import org.apache.beam.sdk.util.PerKeyCombineFnRunners;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypedPValue;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
 * <p>Throws an exception from {@link #run} if execution fails.
 *
 * <p><h3>Permissions</h3>
 * When reading from a Dataflow source or writing to a Dataflow sink using
 * {@code DirectPipelineRunner}, the Cloud Platform account that you configured with the
 * <a href="https://cloud.google.com/sdk/gcloud">gcloud</a> executable will need access to the
 * corresponding source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DirectPipelineRunner
    extends PipelineRunner<DirectPipelineRunner.EvaluationResults> {
  private static final Logger LOG = LoggerFactory.getLogger(DirectPipelineRunner.class);

  /**
   * A source of random data, which can be seeded if determinism is desired.
   */
  private Random rand;

  /**
   * A map from PTransform class to the corresponding
   * TransformEvaluator to use to evaluate that transform.
   *
   * <p>A static map that contains system-wide defaults.
   */
  private static Map<Class, TransformEvaluator> defaultTransformEvaluators =
      new HashMap<>();

  /**
   * A map from PTransform class to the corresponding
   * TransformEvaluator to use to evaluate that transform.
   *
   * <p>An instance map that contains bindings for this DirectPipelineRunner.
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
      TransformEvaluator<? super TransformT> transformEvaluator) {
    if (defaultTransformEvaluators.put(transformClass, transformEvaluator)
        != null) {
      throw new IllegalArgumentException(
          "defining multiple evaluators for " + transformClass);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

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
   * Enable runtime testing to verify that all functions and {@link Coder}
   * instances can be serialized.
   *
   * <p>Enabled by default.
   *
   * <p>This method modifies the {@code DirectPipelineRunner} instance and
   * returns itself.
   */
  public DirectPipelineRunner withSerializabilityTesting(boolean enable) {
    this.testSerializability = enable;
    return this;
  }

  /**
   * Enable runtime testing to verify that all values can be encoded.
   *
   * <p>Enabled by default.
   *
   * <p>This method modifies the {@code DirectPipelineRunner} instance and
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
   * <p>This is accomplished by randomizing the order of elements.
   *
   * <p>Enabled by default.
   *
   * <p>This method modifies the {@code DirectPipelineRunner} instance and
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
    } else if (transform instanceof TextIO.Write.Bound) {
      return (OutputT) applyTextIOWrite((TextIO.Write.Bound) transform, (PCollection<?>) input);
    } else if (transform instanceof AvroIO.Write.Bound) {
      return (OutputT) applyAvroIOWrite((AvroIO.Write.Bound) transform, (PCollection<?>) input);
    } else if (transform instanceof GroupByKey) {
      return (OutputT)
          ((PCollection) input).apply(new GroupByKeyViaGroupByKeyOnly((GroupByKey) transform));
    } else if (transform instanceof Window.Bound) {
      return (OutputT)
          ((PCollection) input).apply(new AssignWindowsAndSetStrategy((Window.Bound) transform));
    } else {
      return super.apply(transform, input);
    }
  }

  private <K, InputT, AccumT, OutputT> PCollection<KV<K, OutputT>> applyTestCombine(
      Combine.GroupedValues<K, InputT, OutputT> transform,
      PCollection<KV<K, Iterable<InputT>>> input) {

    PCollection<KV<K, OutputT>> output = input
        .apply(ParDo.of(TestCombineDoFn.create(transform, input, testSerializability, rand))
            .withSideInputs(transform.getSideInputs()));

    try {
      output.setCoder(transform.getDefaultOutputCoder(input));
    } catch (CannotProvideCoderException exc) {
      // let coder inference occur later, if it can
    }
    return output;
  }

  private static class ElementProcessingOrderPartitionFn<T> implements PartitionFn<T> {
    private int elementNumber;
    @Override
    public int partitionFor(T elem, int numPartitions) {
      return elementNumber++ % numPartitions;
    }
  }

  /**
   * Applies TextIO.Write honoring user requested sharding controls (i.e. withNumShards)
   * by applying a partition function based upon the number of shards the user requested.
   */
  private static class DirectTextIOWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final TextIO.Write.Bound<T> transform;

    private DirectTextIOWrite(TextIO.Write.Bound<T> transform) {
      this.transform = transform;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      checkState(transform.getNumShards() > 1,
          "DirectTextIOWrite is expected to only be used when sharding controls are required.");

      // Evenly distribute all the elements across the partitions.
      PCollectionList<T> partitionedElements =
          input.apply(Partition.of(transform.getNumShards(),
                                   new ElementProcessingOrderPartitionFn<T>()));

      // For each input PCollection partition, create a write transform that represents
      // one of the specific shards.
      for (int i = 0; i < transform.getNumShards(); ++i) {
        /*
         * This logic mirrors the file naming strategy within
         * {@link FileBasedSink#generateDestinationFilenames()}
         */
        String outputFilename = IOChannelUtils.constructName(
            transform.getFilenamePrefix(),
            transform.getShardNameTemplate(),
            getFileExtension(transform.getFilenameSuffix()),
            i,
            transform.getNumShards());

        String transformName = String.format("%s(Shard:%s)", transform.getName(), i);
        partitionedElements.get(i).apply(transformName,
            transform.withNumShards(1).withShardNameTemplate("").withSuffix("").to(outputFilename));
      }
      return PDone.in(input.getPipeline());
    }
  }

  /**
   * Returns the file extension to be used. If the user did not request a file
   * extension then this method returns the empty string. Otherwise this method
   * adds a {@code "."} to the beginning of the users extension if one is not present.
   *
   * <p>This is copied from {@link FileBasedSink} to not expose it.
   */
  private static String getFileExtension(String usersExtension) {
    if (usersExtension == null || usersExtension.isEmpty()) {
      return "";
    }
    if (usersExtension.startsWith(".")) {
      return usersExtension;
    }
    return "." + usersExtension;
  }

  /**
   * Apply the override for TextIO.Write.Bound if the user requested sharding controls
   * greater than one.
   */
  private <T> PDone applyTextIOWrite(TextIO.Write.Bound<T> transform, PCollection<T> input) {
    if (transform.getNumShards() <= 1) {
      // By default, the DirectPipelineRunner outputs to only 1 shard. Since the user never
      // requested sharding controls greater than 1, we default to outputting to 1 file.
      return super.apply(transform.withNumShards(1), input);
    }
    return input.apply(new DirectTextIOWrite<>(transform));
  }

  /**
   * Applies AvroIO.Write honoring user requested sharding controls (i.e. withNumShards)
   * by applying a partition function based upon the number of shards the user requested.
   */
  private static class DirectAvroIOWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final AvroIO.Write.Bound<T> transform;

    private DirectAvroIOWrite(AvroIO.Write.Bound<T> transform) {
      this.transform = transform;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      checkState(transform.getNumShards() > 1,
          "DirectAvroIOWrite is expected to only be used when sharding controls are required.");

      // Evenly distribute all the elements across the partitions.
      PCollectionList<T> partitionedElements =
          input.apply(Partition.of(transform.getNumShards(),
                                   new ElementProcessingOrderPartitionFn<T>()));

      // For each input PCollection partition, create a write transform that represents
      // one of the specific shards.
      for (int i = 0; i < transform.getNumShards(); ++i) {
        /*
         * This logic mirrors the file naming strategy within
         * {@link FileBasedSink#generateDestinationFilenames()}
         */
        String outputFilename = IOChannelUtils.constructName(
            transform.getFilenamePrefix(),
            transform.getShardNameTemplate(),
            getFileExtension(transform.getFilenameSuffix()),
            i,
            transform.getNumShards());

        String transformName = String.format("%s(Shard:%s)", transform.getName(), i);
        partitionedElements.get(i).apply(transformName,
            transform.withNumShards(1).withShardNameTemplate("").withSuffix("").to(outputFilename));
      }
      return PDone.in(input.getPipeline());
    }
  }

  private static class AssignWindowsAndSetStrategy<T, W extends BoundedWindow>
      extends PTransform<PCollection<T>, PCollection<T>> {

    private final Window.Bound<T> wrapped;

    public AssignWindowsAndSetStrategy(Window.Bound<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public PCollection<T> apply(PCollection<T> input) {
      WindowingStrategy<?, ?> outputStrategy =
          wrapped.getOutputStrategyInternal(input.getWindowingStrategy());

      WindowFn<T, BoundedWindow> windowFn =
          (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();

      // If the Window.Bound transform only changed parts other than the WindowFn, then
      // we skip AssignWindows even though it should be harmless in a perfect world.
      // The world is not perfect, and a GBK may have set it to InvalidWindows to forcibly
      // crash if another GBK is performed without explicitly setting the WindowFn. So we skip
      // AssignWindows in this case.
      if (wrapped.getWindowFn() == null) {
        return input.apply("Identity", ParDo.of(new IdentityFn<T>()))
            .setWindowingStrategyInternal(outputStrategy);
      } else {
        return input
            .apply("AssignWindows", new AssignWindows<T, BoundedWindow>(windowFn))
            .setWindowingStrategyInternal(outputStrategy);
      }
    }
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  /**
   * Apply the override for AvroIO.Write.Bound if the user requested sharding controls
   * greater than one.
   */
  private <T> PDone applyAvroIOWrite(AvroIO.Write.Bound<T> transform, PCollection<T> input) {
    if (transform.getNumShards() <= 1) {
      // By default, the DirectPipelineRunner outputs to only 1 shard. Since the user never
      // requested sharding controls greater than 1, we default to outputting to 1 file.
      return super.apply(transform.withNumShards(1), input);
    }
    return input.apply(new DirectAvroIOWrite<>(transform));
  }

  /**
   * The implementation may split the {@link KeyedCombineFn} into ADD, MERGE and EXTRACT phases (
   * see {@code org.apache.beam.sdk.runners.worker.CombineValuesFn}). In order to emulate
   * this for the {@link DirectPipelineRunner} and provide an experience closer to the service, go
   * through heavy serializability checks for the equivalent of the results of the ADD phase, but
   * after the {@link org.apache.beam.sdk.transforms.GroupByKey} shuffle, and the MERGE
   * phase. Doing these checks ensure that not only is the accumulator coder serializable, but
   * the accumulator coder can actually serialize the data in question.
   */
  public static class TestCombineDoFn<K, InputT, AccumT, OutputT>
      extends DoFn<KV<K, Iterable<InputT>>, KV<K, OutputT>> {
    private final PerKeyCombineFnRunner<? super K, ? super InputT, AccumT, OutputT> fnRunner;
    private final Coder<AccumT> accumCoder;
    private final boolean testSerializability;
    private final Random rand;

    public static <K, InputT, AccumT, OutputT> TestCombineDoFn<K, InputT, AccumT, OutputT> create(
        Combine.GroupedValues<K, InputT, OutputT> transform,
        PCollection<KV<K, Iterable<InputT>>> input,
        boolean testSerializability,
        Random rand) {

      AppliedCombineFn<? super K, ? super InputT, ?, OutputT> fn = transform.getAppliedFn(
          input.getPipeline().getCoderRegistry(), input.getCoder(), input.getWindowingStrategy());

      return new TestCombineDoFn(
          PerKeyCombineFnRunners.create(fn.getFn()),
          fn.getAccumulatorCoder(),
          testSerializability,
          rand);
    }

    public TestCombineDoFn(
        PerKeyCombineFnRunner<? super K, ? super InputT, AccumT, OutputT> fnRunner,
        Coder<AccumT> accumCoder,
        boolean testSerializability,
        Random rand) {
      this.fnRunner = fnRunner;
      this.accumCoder = accumCoder;
      this.testSerializability = testSerializability;
      this.rand = rand;

      // Check that this does not crash, specifically to catch anonymous CustomCoder subclasses.
      this.accumCoder.getEncodingId();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      K key = c.element().getKey();
      Iterable<InputT> values = c.element().getValue();
      List<AccumT> groupedPostShuffle =
          ensureSerializableByCoder(ListCoder.of(accumCoder),
              addInputsRandomly(fnRunner, key, values, rand, c),
              "After addInputs of KeyedCombineFn " + fnRunner.fn().toString());
      AccumT merged =
          ensureSerializableByCoder(accumCoder,
            fnRunner.mergeAccumulators(key, groupedPostShuffle, c),
            "After mergeAccumulators of KeyedCombineFn " + fnRunner.fn().toString());
      // Note: The serializability of KV<K, OutputT> is ensured by the
      // runner itself, since it's a transform output.
      c.output(KV.of(key, fnRunner.extractOutput(key, merged, c)));
    }

    /**
     * Create a random list of accumulators from the given list of values.
     *
     * <p>Visible for testing purposes only.
     */
    public static <K, AccumT, InputT> List<AccumT> addInputsRandomly(
        PerKeyCombineFnRunner<? super K, ? super InputT, AccumT, ?> fnRunner,
        K key,
        Iterable<InputT> values,
        Random random,
        DoFn<?, ?>.ProcessContext c) {
      List<AccumT> out = new ArrayList<AccumT>();
      int i = 0;
      AccumT accumulator = fnRunner.createAccumulator(key, c);
      boolean hasInput = false;

      for (InputT value : values) {
        accumulator = fnRunner.addInput(key, accumulator, value, c);
        hasInput = true;

        // For each index i, flip a 1/2^i weighted coin for whether to
        // create a new accumulator after index i is added, i.e. [0]
        // is guaranteed, [1] is an even 1/2, [2] is 1/4, etc. The
        // goal is to partition the inputs into accumulators, and make
        // the accumulators potentially lumpy.  Also compact about half
        // of the accumulators.
        if (i == 0 || random.nextInt(1 << Math.min(i, 30)) == 0) {
          if (i % 2 == 0) {
            accumulator = fnRunner.compact(key, accumulator, c);
          }
          out.add(accumulator);
          accumulator = fnRunner.createAccumulator(key, c);
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

    Evaluator evaluator = new Evaluator(rand);
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
     * Note that within the {@link org.apache.beam.sdk.transforms.DoFn.Context}
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
     * persisted state separate across keys.
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
     * <p>Error context is prefixed to any thrown exceptions.
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

  class Evaluator extends PipelineVisitor.Defaults implements EvaluationContext {
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

    private Random rand;

    public Evaluator() {
      this(new Random());
    }

    public Evaluator(Random rand) {
      this.rand = rand;
    }

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
    public void visitPrimitiveTransform(TransformTreeNode node) {
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
            "internal error: expecting a PValue to have a producingTransform");
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
            "internal error: setting the value of " + pvalue
            + " more than once");
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
            "internal error: getting the value of " + pvalue
            + " before it has been computed");
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
      ensurePCollectionEncodable(pc, elements);
      setPValue(pc, elements);
    }

    @Override
    public <ElemT, T, WindowedT> void setPCollectionView(
        PCollectionView<T> view,
        Iterable<WindowedValue<ElemT>> value) {
      LOG.debug("Setting {} = {}", view, value);
      setPValue(view, value);
    }

    /**
     * Retrieves the value of the given {@link PCollection}.
     * Throws an exception if the {@link PCollection}'s value hasn't already been set.
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
     * If {@code testEncodability}, ensures that the {@link PCollection}'s coder and elements are
     * encodable and decodable by encoding them and decoding them, and returning the result.
     * Otherwise returns the argument elements.
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

  /**
   * The key by which GBK groups inputs - elements are grouped by the encoded form of the key,
   * but the original key may be accessed as well.
   */
  private static class GroupingKey<K> {
    private K key;
    private byte[] encodedKey;

    public GroupingKey(K key, byte[] encodedKey) {
      this.key = key;
      this.encodedKey = encodedKey;
    }

    public K getKey() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof GroupingKey) {
        GroupingKey<?> that = (GroupingKey<?>) o;
        return Arrays.equals(this.encodedKey, that.encodedKey);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(encodedKey);
    }
  }

  private final DirectPipelineOptions options;
  private boolean testSerializability;
  private boolean testEncodability;
  private boolean testUnorderedness;

  /** Returns a new DirectPipelineRunner. */
  private DirectPipelineRunner(DirectPipelineOptions options) {
    this.options = options;
    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerStandardIOFactories(options);
    long randomSeed;
    if (options.getDirectPipelineRunnerRandomSeed() != null) {
      randomSeed = options.getDirectPipelineRunnerRandomSeed();
    } else {
      randomSeed = new Random().nextLong();
    }

    LOG.debug("DirectPipelineRunner using random seed {}.", randomSeed);
    rand = new Random(randomSeed);

    testSerializability = options.isTestSerializability();
    testEncodability = options.isTestEncodability();
    testUnorderedness = options.isTestUnorderedness();
  }

  /**
   * Get the options used in this {@link Pipeline}.
   */
  public DirectPipelineOptions getPipelineOptions() {
    return options;
  }

  @Override
  public String toString() {
    return "DirectPipelineRunner#" + hashCode();
  }

  public static <K, V> void evaluateGroupByKeyOnly(
      GroupByKeyOnly<K, V> transform,
      EvaluationContext context) {
    PCollection<KV<K, V>> input = context.getInput(transform);

    List<ValueWithMetadata<KV<K, V>>> inputElems =
        context.getPCollectionValuesWithMetadata(input);

    Coder<K> keyCoder = GroupByKey.getKeyCoder(input.getCoder());

    Map<GroupingKey<K>, List<V>> groupingMap = new HashMap<>();

    for (ValueWithMetadata<KV<K, V>> elem : inputElems) {
      K key = elem.getValue().getKey();
      V value = elem.getValue().getValue();
      byte[] encodedKey;
      try {
        encodedKey = encodeToByteArray(keyCoder, key);
      } catch (CoderException exn) {
        // TODO: Put in better element printing:
        // truncate if too long.
        throw new IllegalArgumentException(
            "unable to encode key " + key + " of input to " + transform
            + " using " + keyCoder,
            exn);
      }
      GroupingKey<K> groupingKey =
          new GroupingKey<>(key, encodedKey);
      List<V> values = groupingMap.get(groupingKey);
      if (values == null) {
        values = new ArrayList<V>();
        groupingMap.put(groupingKey, values);
      }
      values.add(value);
    }

    List<ValueWithMetadata<KV<K, Iterable<V>>>> outputElems =
        new ArrayList<>();
    for (Map.Entry<GroupingKey<K>, List<V>> entry : groupingMap.entrySet()) {
      GroupingKey<K> groupingKey = entry.getKey();
      K key = groupingKey.getKey();
      List<V> values = entry.getValue();
      values = context.randomizeIfUnordered(values, true /* inPlaceAllowed */);
      outputElems.add(ValueWithMetadata
                      .of(WindowedValue.valueInEmptyWindows(KV.<K, Iterable<V>>of(key, values)))
                      .withKey(key));
    }

    context.setPCollectionValuesWithMetadata(context.getOutput(transform),
                                             outputElems);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public
  static <K, V> void registerGroupByKeyOnly() {
    registerDefaultTransformEvaluator(
        GroupByKeyOnly.class,
        new TransformEvaluator<GroupByKeyOnly>() {
          @Override
          public void evaluate(
              GroupByKeyOnly transform,
              EvaluationContext context) {
            evaluateGroupByKeyOnly(transform, context);
          }
        });
  }

  static {
    registerGroupByKeyOnly();
  }

}
