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

package org.apache.beam.runners.spark.translation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.spark.Accumulator;

/**
 * The SparkRuntimeContext allows us to define useful features on the client side before our
 * data flow program is launched.
 */
public class SparkRuntimeContext implements Serializable {
  private final String serializedPipelineOptions;
  private transient CoderRegistry coderRegistry;

  // map for names to Beam aggregators.
  private final Map<String, Aggregator<?, ?>> aggregators = new HashMap<>();

  SparkRuntimeContext(Pipeline pipeline) {
    this.serializedPipelineOptions = serializePipelineOptions(pipeline.getOptions());
  }

  private String serializePipelineOptions(PipelineOptions pipelineOptions) {
    try {
      return new ObjectMapper().writeValueAsString(pipelineOptions);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize the pipeline options.", e);
    }
  }

  private static PipelineOptions deserializePipelineOptions(String serializedPipelineOptions) {
    try {
      return new ObjectMapper().readValue(serializedPipelineOptions, PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize the pipeline options.", e);
    }
  }

  public PipelineOptions getPipelineOptions() {
    return PipelineOptionsHolder.getOrInit(serializedPipelineOptions);
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param accum     Spark Accumulator.
   * @param named     Name of aggregator.
   * @param combineFn Combine function used in aggregation.
   * @param <InputT>  Type of inputs to aggregator.
   * @param <InterT>  Intermediate data type
   * @param <OutputT> Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <InputT, InterT, OutputT> Aggregator<InputT, OutputT> createAggregator(
      Accumulator<NamedAggregators> accum,
      String named,
      Combine.CombineFn<? super InputT, InterT, OutputT> combineFn) {
    @SuppressWarnings("unchecked")
    Aggregator<InputT, OutputT> aggregator = (Aggregator<InputT, OutputT>) aggregators.get(named);
    try {
      if (aggregator == null) {
        @SuppressWarnings("unchecked")
        final
        NamedAggregators.CombineFunctionState<InputT, InterT, OutputT> state =
            new NamedAggregators.CombineFunctionState<>(
                (Combine.CombineFn<InputT, InterT, OutputT>) combineFn,
                // hidden assumption: InputT == OutputT
                (Coder<InputT>) getCoderRegistry().getCoder(combineFn.getOutputType()),
                this);

        accum.add(new NamedAggregators(named, state));
        aggregator = new SparkAggregator<>(named, state);
        aggregators.put(named, aggregator);
      }
      return aggregator;
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(String.format("Unable to create an aggregator named: [%s]", named),
                                 e);
    }
  }

  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = new CoderRegistry();
      coderRegistry.registerStandardCoders();
    }
    return coderRegistry;
  }

  private static class PipelineOptionsHolder {
    // on executors, this should deserialize once.
    private static transient volatile PipelineOptions pipelineOptions = null;

    static PipelineOptions getOrInit(String serializedPipelineOptions) {
      if (pipelineOptions == null) {
        synchronized (PipelineOptionsHolder.class) {
          if (pipelineOptions == null) {
            pipelineOptions = deserializePipelineOptions(serializedPipelineOptions);
          }
        }
        // register IO factories.
        IOChannelUtils.registerIOFactoriesAllowOverride(pipelineOptions);
        FileSystems.setDefaultConfigInWorkers(pipelineOptions);
      }
      return pipelineOptions;
    }
  }

  /**
   * Initialize spark aggregators exactly once.
   *
   * @param <InputT> Type of element fed in to aggregator.
   */
  private static class SparkAggregator<InputT, OutputT>
      implements Aggregator<InputT, OutputT>, Serializable {
    private final String name;
    private final NamedAggregators.State<InputT, ?, OutputT> state;

    SparkAggregator(String name, NamedAggregators.State<InputT, ?, OutputT> state) {
      this.name = name;
      this.state = state;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void addValue(InputT elem) {
      state.update(elem);
    }

    @Override
    public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
      return state.getCombineFn();
    }
  }
}
