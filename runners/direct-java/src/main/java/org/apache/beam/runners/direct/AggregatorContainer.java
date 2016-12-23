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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * AccumT container for the current values associated with {@link Aggregator Aggregators}.
 */
public class AggregatorContainer {

  private static class AggregatorInfo<InputT, AccumT, OutputT>
      implements Aggregator<InputT, OutputT> {
    private final String stepName;
    private final String name;
    private final CombineFn<InputT, AccumT, OutputT> combiner;
    @GuardedBy("this")
    private volatile AccumT accumulator = null;
    private boolean committed = false;

    private AggregatorInfo(
        String stepName, String name, CombineFn<InputT, AccumT, OutputT> combiner) {
      this.stepName = stepName;
      this.name = name;
      this.combiner = combiner;
    }

    @Override
    public synchronized void addValue(InputT input) {
      checkState(!committed, "Cannot addValue after committing");
      if (accumulator == null) {
        accumulator = combiner.createAccumulator();
      }
      accumulator = combiner.addInput(accumulator, input);
    }

    public synchronized OutputT getOutput() {
      return accumulator == null ? null : combiner.extractOutput(accumulator);
    }

    private void merge(AggregatorInfo<?, ?, ?> other) {
      // Aggregators are only merged if they are the same (same step, same name).
      // As a result, they should also have the same CombineFn, so this is safe.
      AggregatorInfo<InputT, AccumT, OutputT> otherSafe =
          (AggregatorInfo<InputT, AccumT, OutputT>) other;
      mergeSafe(otherSafe);
    }

    private synchronized void mergeSafe(AggregatorInfo<InputT, AccumT, OutputT> other) {
      if (accumulator == null) {
        accumulator = other.accumulator;
      } else if (other.accumulator != null) {
        accumulator = combiner.mergeAccumulators(Arrays.asList(accumulator, other.accumulator));
      }
    }

    public String getStepName() {
      return name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public CombineFn<InputT, ?, OutputT> getCombineFn() {
      return combiner;
    }
  }

  private final ConcurrentMap<AggregatorKey, AggregatorInfo<?, ?, ?>> accumulators =
      new ConcurrentHashMap<>();

  private AggregatorContainer() {
  }

  public static AggregatorContainer create() {
    return new AggregatorContainer();
  }

  @Nullable
  <OutputT> OutputT getAggregate(String stepName, String aggregatorName) {
    AggregatorInfo<?, ?, OutputT> aggregatorInfo =
        (AggregatorInfo<?, ?, OutputT>) accumulators.get(
            AggregatorKey.create(stepName, aggregatorName));
    return aggregatorInfo == null ? null : aggregatorInfo.getOutput();
  }

  public Mutator createMutator() {
    return new Mutator(this);
  }

  /**
   * AccumT class for mutations to the aggregator values.
   */
  public static class Mutator implements AggregatorFactory {

    private final Map<AggregatorKey, AggregatorInfo<?, ?, ?>> accumulatorDeltas = new HashMap<>();
    private final AggregatorContainer container;
    private boolean committed = false;

    private Mutator(AggregatorContainer container) {
      this.container = container;
    }

    public void commit() {
      checkState(!committed, "Should not be already committed");
      committed = true;

      for (Map.Entry<AggregatorKey, AggregatorInfo<?, ?, ?>> entry : accumulatorDeltas.entrySet()) {
        AggregatorInfo<?, ?, ?> previous = container.accumulators.get(entry.getKey());
        entry.getValue().committed = true;
        if (previous == null) {
          previous = container.accumulators.putIfAbsent(entry.getKey(), entry.getValue());
        }
        if (previous != null) {
          previous.merge(entry.getValue());
          previous.committed = true;
        }
      }
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass,
        ExecutionContext.StepContext step,
        String name,
        CombineFn<InputT, AccumT, OutputT> combine) {
      return createAggregatorForStep(step, name, combine);
    }

    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createSystemAggregator(
        ExecutionContext.StepContext step,
        String name,
        CombineFn<InputT, AccumT, OutputT> combiner) {
      return createAggregatorForStep(step, name, combiner);
    }

    private <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForStep(
        ExecutionContext.StepContext step,
        String name,
        CombineFn<InputT, AccumT, OutputT> combine) {
      checkState(!committed, "Cannot create aggregators after committing");
      AggregatorKey key = AggregatorKey.create(step.getStepName(), name);
      AggregatorInfo<?, ?, ?> aggregatorInfo = accumulatorDeltas.get(key);
      if (aggregatorInfo != null) {
        AggregatorInfo<InputT, ?, OutputT> typedAggregatorInfo =
            (AggregatorInfo<InputT, ?, OutputT>) aggregatorInfo;
        return typedAggregatorInfo;
      } else {
        AggregatorInfo<InputT, ?, OutputT> typedAggregatorInfo =
            new AggregatorInfo<>(step.getStepName(), name, combine);
        accumulatorDeltas.put(key, typedAggregatorInfo);
        return typedAggregatorInfo;
      }
    }
  }

  /**
   * Aggregators are identified by a step name and an aggregator name.
   */
  @AutoValue
  public abstract static class AggregatorKey {
    public static AggregatorKey create(String stepName, String aggregatorName)  {
      return new AutoValue_AggregatorContainer_AggregatorKey(stepName, aggregatorName);
    }

    public abstract String getStepName();
    public abstract String aggregatorName();
  }
}
