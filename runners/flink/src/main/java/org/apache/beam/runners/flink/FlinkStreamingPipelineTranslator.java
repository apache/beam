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
package org.apache.beam.runners.flink;

import static org.apache.beam.runners.core.construction.PTransformTranslation.WRITE_FILES_TRANSFORM_URN;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.runners.core.construction.WriteFilesTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardingFunction;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a {@link FlinkPipelineTranslator} for streaming jobs. Its role is to translate the
 * user-provided {@link org.apache.beam.sdk.values.PCollection}-based job into a {@link
 * org.apache.flink.streaming.api.datastream.DataStream} one.
 */
class FlinkStreamingPipelineTranslator extends FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamingPipelineTranslator.class);

  /** The necessary context in the case of a straming job. */
  private final FlinkStreamingTranslationContext streamingContext;

  private int depth = 0;

  public FlinkStreamingPipelineTranslator(StreamExecutionEnvironment env, PipelineOptions options) {
    this.streamingContext = new FlinkStreamingTranslationContext(env, options);
  }

  @Override
  public void translate(Pipeline pipeline) {
    // Ensure all outputs of all reads are consumed.
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    super.translate(pipeline);
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    this.depth++;

    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      StreamTransformTranslator<?> translator =
          FlinkStreamingTransformTranslators.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyStreamingTransform(transform, node, translator);
        LOG.info("{} translated- {}", genSpaces(this.depth), node.getFullName());
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    this.depth--;
    LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());
    // get the transformation corresponding to hte node we are
    // currently visiting and translate it into its Flink alternative.

    PTransform<?, ?> transform = node.getTransform();
    StreamTransformTranslator<?> translator =
        FlinkStreamingTransformTranslators.getTranslator(transform);

    if (translator == null || !applyCanTranslate(transform, node, translator)) {
      String transformUrn = PTransformTranslation.urnForTransform(transform);
      LOG.info(transformUrn);
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    applyStreamingTransform(transform, node, translator);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    // do nothing here
  }

  private <T extends PTransform<?, ?>> void applyStreamingTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      StreamTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    StreamTransformTranslator<T> typedTranslator = (StreamTransformTranslator<T>) translator;

    // create the applied PTransform on the streamingContext
    streamingContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    typedTranslator.translateNode(typedTransform, streamingContext);
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      StreamTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    StreamTransformTranslator<T> typedTranslator = (StreamTransformTranslator<T>) translator;

    streamingContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));

    return typedTranslator.canTranslate(typedTransform, streamingContext);
  }

  /**
   * The interface that every Flink translator of a Beam operator should implement. This interface
   * is for <b>streaming</b> jobs. For examples of such translators see {@link
   * FlinkStreamingTransformTranslators}.
   */
  abstract static class StreamTransformTranslator<T extends PTransform> {

    /** Translate the given transform. */
    abstract void translateNode(T transform, FlinkStreamingTranslationContext context);

    /** Returns true iff this translator can translate the given transform. */
    boolean canTranslate(T transform, FlinkStreamingTranslationContext context) {
      return true;
    }
  }

  static PTransformMatcher writeFilesNeedsOverrides() {
    return application -> {
      if (WRITE_FILES_TRANSFORM_URN.equals(
          PTransformTranslation.urnForTransformOrNull(application.getTransform()))) {
        try {
          FlinkPipelineOptions options =
              application.getPipeline().getOptions().as(FlinkPipelineOptions.class);
          ShardingFunction shardingFn =
              WriteFilesTranslation.getShardingFunction((AppliedPTransform) application);
          return WriteFilesTranslation.isRunnerDeterminedSharding((AppliedPTransform) application)
              || (options.isAutoBalanceWriteFilesShardingEnabled() && shardingFn == null);
        } catch (IOException exc) {
          throw new RuntimeException(
              String.format(
                  "Transform with URN %s failed to parse: %s",
                  WRITE_FILES_TRANSFORM_URN, application.getTransform()),
              exc);
        }
      }
      return false;
    };
  }

  @VisibleForTesting
  static class StreamingShardedWriteFactory<UserT, DestinationT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<UserT>,
          WriteFilesResult<DestinationT>,
          WriteFiles<UserT, DestinationT, OutputT>> {
    FlinkPipelineOptions options;

    StreamingShardedWriteFactory(PipelineOptions options) {
      this.options = options.as(FlinkPipelineOptions.class);
    }

    @Override
    public PTransformReplacement<PCollection<UserT>, WriteFilesResult<DestinationT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<UserT>,
                    WriteFilesResult<DestinationT>,
                    WriteFiles<UserT, DestinationT, OutputT>>
                transform) {
      // By default, if numShards is not set WriteFiles will produce one file per bundle. In
      // streaming, there are large numbers of small bundles, resulting in many tiny files.
      // Instead we pick parallelism * 2 to ensure full parallelism, but prevent too-many files.
      Integer jobParallelism = options.getParallelism();

      Preconditions.checkArgument(
          jobParallelism > 0,
          "Parallelism of a job should be greater than 0. Currently set: %s",
          jobParallelism);
      int numShards = jobParallelism * 2;

      try {
        List<PCollectionView<?>> sideInputs =
            WriteFilesTranslation.getDynamicDestinationSideInputs(transform);
        FileBasedSink sink = WriteFilesTranslation.getSink(transform);

        @SuppressWarnings("unchecked")
        WriteFiles<UserT, DestinationT, OutputT> replacement =
            WriteFiles.to(sink).withSideInputs(sideInputs);
        if (WriteFilesTranslation.isWindowedWrites(transform)) {
          replacement = replacement.withWindowedWrites();
        }

        if (options.isAutoBalanceWriteFilesShardingEnabled()) {
          Preconditions.checkArgument(
              options.getParallelism() > 0,
              "Parallelism is required to be set in FlinkPipelineOptions when isAutoBalanceWriteFilesShardingEnabled");
          Preconditions.checkArgument(
              options.getMaxParallelism() > 0,
              "MaxParallelism is required to be set in FlinkPipelineOptions when isAutoBalanceWriteFilesShardingEnabled");

          replacement =
              replacement.withShardingFunction(
                  new FlinkAutoBalancedShardKeyShardingFunction<>(
                      options.getParallelism(),
                      options.getMaxParallelism(),
                      sink.getDynamicDestinations().getDestinationCoder()));
        }

        return PTransformReplacement.of(
            PTransformReplacements.getSingletonMainInput(transform),
            replacement.withNumShards(numShards));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, WriteFilesResult<DestinationT> newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  /**
   * Flink has a known problem of unevenly assigning keys to key groups (and then workers) for cases
   * that number of keys is not >> key groups. This is typical scenario when writing files, where
   * one do not want to end up with too many small files. This {@link ShardingFunction} implements
   * what was suggested on Flink's <a
   * href="http://mail-archives.apache.org/mod_mbox/flink-dev/201901.mbox/%3CCAOUjMkygmFMDbOJNCmndrZ0bYug=iQJmVz6QvMW7C87n=pw8SQ@mail.gmail.com%3E">mailing
   * list</a> and properly chooses shard keys in a way that they are distributed evenly among
   * workers by Flink.
   */
  private static class FlinkAutoBalancedShardKeyShardingFunction<UserT, DestinationT>
      implements ShardingFunction<UserT, DestinationT> {

    private final int parallelism;
    private final int maxParallelism;
    private final Coder<DestinationT> destinationCoder;
    private final Map<CacheKey, ShardedKey<Integer>> cache = new HashMap<>();
    private final Set<Integer> usedSalts = new HashSet<>();

    private int shardNumber = -1;

    private FlinkAutoBalancedShardKeyShardingFunction(
        int parallelism, int maxParallelism, Coder<DestinationT> destinationCoder) {
      this.parallelism = parallelism;
      this.maxParallelism = maxParallelism;
      this.destinationCoder = destinationCoder;
    }

    @Override
    public ShardedKey<Integer> assignShardKey(
        DestinationT destination, UserT element, int shardCount) throws Exception {

      if (shardNumber == -1) {
        shardNumber = ThreadLocalRandom.current().nextInt(shardCount);
      } else {
        shardNumber = (shardNumber + 1) % shardCount;
      }

      int destinationKey =
          Arrays.hashCode(CoderUtils.encodeToByteArray(destinationCoder, destination));
      // we need to ensure that keys are always stable no matter at which worker they
      // are created and what is an order of observed shard numbers
      if (cache.size() < shardNumber) {
        for (int i = 0; i < shardNumber; i++) {
          generateInternal(new CacheKey(destinationKey, i));
        }
      }

      return generateInternal(new CacheKey(destinationKey, shardNumber));
    }

    private ShardedKey<Integer> generateInternal(CacheKey key) {

      ShardedKey<Integer> result = cache.get(key);
      if (result != null) {
        return result;
      }

      int salt = -1;
      while (true) {
        salt++;
        ShardedKey<Integer> shk = ShardedKey.of(Objects.hash(key.key, salt), key.shard);
        int targetPartition = key.shard % parallelism;

        // create effective key in the same way Beam/Flink will do so we can see if it gets
        // allocated to the partition we want
        ByteBuffer effectiveKey;
        try {
          byte[] bytes = CoderUtils.encodeToByteArray(ShardedKeyCoder.of(VarIntCoder.of()), shk);
          effectiveKey = ByteBuffer.wrap(bytes);
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }

        int partition =
            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                effectiveKey, maxParallelism, parallelism);

        if (partition == targetPartition && !usedSalts.contains(salt)) {
          usedSalts.add(salt);
          cache.put(key, shk);
          return shk;
        }
      }
    }

    private static class CacheKey implements Serializable {
      private final int key;
      private final int shard;

      private CacheKey(int key, int shard) {
        this.key = key;
        this.shard = shard;
      }

      @Override
      public int hashCode() {
        return Objects.hash(key, shard);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof CacheKey) {
          CacheKey o = (CacheKey) obj;
          return o.key == key && o.shard == shard;
        }
        return false;
      }
    }
  }
}
