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
package org.apache.beam.sdk.ml.inference.remote;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.io.components.throttling.ReactiveThrottler;
import org.apache.beam.sdk.transforms.BatchElements;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} for making remote inference calls to external machine learning services.
 *
 * <p>{@code RemoteInference} provides a framework for integrating remote ML model inference into
 * Apache Beam pipelines and handles the communication between pipelines and external inference
 * APIs.
 *
 * <h3>Example: OpenAI Model Inference</h3>
 *
 * <pre>{@code
 * // Create model parameters
 * OpenAIModelParameters params = OpenAIModelParameters.builder()
 *     .apiKey("your-api-key")
 *     .modelName("gpt-4")
 *     .instructionPrompt("Analyse sentiment as positive or negative")
 *     .build();
 *
 * // Apply remote inference transform
 * PCollection<OpenAIModelInput> inputs = pipeline.apply(Create.of(
 *     OpenAIModelInput.create("An excellent B2B SaaS solution that streamlines business processes efficiently."),
 *     OpenAIModelInput.create("Really impressed with the innovative features!")
 * ));
 *
 * PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results =
 *     inputs.apply(
 *         RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
 *             .handler(OpenAIModelHandler.class)
 *             .withParameters(params)
 *     );
 * }</pre>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class RemoteInference {

  /** Invoke the model handler with model parameters. */
  public static <InputT extends BaseInput, OutputT extends BaseResponse>
      Invoke<InputT, OutputT> invoke() {
    return new AutoValue_RemoteInference_Invoke.Builder<InputT, OutputT>() // .setParameters(null)
        .build();
  }

  private RemoteInference() {}

  @AutoValue
  public abstract static class Invoke<InputT extends BaseInput, OutputT extends BaseResponse>
      extends PTransform<
          PCollection<InputT>, PCollection<Iterable<PredictionResult<InputT, OutputT>>>> {

    abstract @Nullable Class<? extends BaseModelHandler<?, InputT, OutputT>> handler();

    abstract @Nullable BaseModelParameters parameters();

    abstract BatchElements.@Nullable BatchConfig batchConfig();

    abstract @Nullable Integer throttleDelaySecs();

    abstract @Nullable Long samplePeriodMs();

    abstract @Nullable Long sampleUpdateMs();

    abstract @Nullable Double overloadRatio();

    abstract Builder<InputT, OutputT> builder();

    @AutoValue.Builder
    abstract static class Builder<InputT extends BaseInput, OutputT extends BaseResponse> {

      abstract Builder<InputT, OutputT> setHandler(
          Class<? extends BaseModelHandler<?, InputT, OutputT>> modelHandler);

      abstract Builder<InputT, OutputT> setParameters(BaseModelParameters modelParameters);

      abstract Builder<InputT, OutputT> setBatchConfig(BatchElements.BatchConfig batchConfig);

      abstract Builder<InputT, OutputT> setThrottleDelaySecs(Integer throttleDelaySecs);

      abstract Builder<InputT, OutputT> setSamplePeriodMs(Long samplePeriodMs);

      abstract Builder<InputT, OutputT> setSampleUpdateMs(Long sampleUpdateMs);

      abstract Builder<InputT, OutputT> setOverloadRatio(Double overloadRatio);

      abstract Invoke<InputT, OutputT> build();
    }

    /** Model handler class for inference. */
    public Invoke<InputT, OutputT> handler(
        Class<? extends BaseModelHandler<?, InputT, OutputT>> modelHandler) {
      return builder().setHandler(modelHandler).build();
    }

    /** Configures the parameters for model initialization. */
    public Invoke<InputT, OutputT> withParameters(BaseModelParameters modelParameters) {
      return builder().setParameters(modelParameters).build();
    }

    /** Configures the batching behavior for the inputs. */
    public Invoke<InputT, OutputT> withBatchConfig(BatchElements.BatchConfig batchConfig) {
      return builder().setBatchConfig(batchConfig).build();
    }

    /** Configures the throttling delay when the client is preemptively throttled. */
    public Invoke<InputT, OutputT> withThrottleDelaySecs(int throttleDelaySecs) {
      checkArgument(throttleDelaySecs >= 0, "throttleDelaySecs must be non-negative");
      return builder().setThrottleDelaySecs(throttleDelaySecs).build();
    }

    /** Configures the length of history to consider when setting throttling probability. */
    public Invoke<InputT, OutputT> withSamplePeriodMs(long samplePeriodMs) {
      checkArgument(samplePeriodMs > 0, "samplePeriodMs must be positive");
      return builder().setSamplePeriodMs(samplePeriodMs).build();
    }

    /** Configures the granularity of time buckets that we store data in for throttling. */
    public Invoke<InputT, OutputT> withSampleUpdateMs(long sampleUpdateMs) {
      checkArgument(sampleUpdateMs > 0, "sampleUpdateMs must be positive");
      return builder().setSampleUpdateMs(sampleUpdateMs).build();
    }

    /** Configures the target ratio between requests sent and successful requests. */
    public Invoke<InputT, OutputT> withOverloadRatio(double overloadRatio) {
      checkArgument(overloadRatio > 0, "overloadRatio must be positive");
      return builder().setOverloadRatio(overloadRatio).build();
    }

    @Override
    public PCollection<Iterable<PredictionResult<InputT, OutputT>>> expand(
        PCollection<InputT> input) {
      checkArgument(handler() != null, "handler() is required");
      checkArgument(parameters() != null, "withParameters() is required");

      BatchElements.BatchConfig config = batchConfig();
      PCollection<List<InputT>> batchedInput;
      if (config != null) {
        batchedInput = input.apply("BatchElements", BatchElements.withConfig(config));
      } else {
        batchedInput = input.apply("BatchElements", BatchElements.withDefaults());
      }

      return batchedInput
          // Pass the list to the inference function
          .apply("RemoteInference", ParDo.of(new RemoteInferenceFn<InputT, OutputT>(this)));
    }

    /**
     * A {@link DoFn} that performs remote inference operation.
     *
     * <p>This function manages the lifecycle of the model handler:
     *
     * <ul>
     *   <li>Instantiates the handler during {@link Setup}
     *   <li>Initializes the remote client via {@link BaseModelHandler#createClient}
     *   <li>Processes elements by calling {@link BaseModelHandler#request}
     * </ul>
     */
    @SuppressWarnings("nullness")
    static class RemoteInferenceFn<InputT extends BaseInput, OutputT extends BaseResponse>
        extends DoFn<List<InputT>, Iterable<PredictionResult<InputT, OutputT>>> {

      private final Class<? extends BaseModelHandler<?, InputT, OutputT>> handlerClass;
      private final BaseModelParameters parameters;
      private transient @Nullable BaseModelHandler modelHandler;
      private final RetryHandler retryHandler;
      private final int throttleDelaySecs;
      private final long samplePeriodMs;
      private final long sampleUpdateMs;
      private final double overloadRatio;
      private transient @Nullable ReactiveThrottler throttler;

      RemoteInferenceFn(Invoke<InputT, OutputT> spec) {
        this.handlerClass = spec.handler();
        this.parameters = spec.parameters();
        this.throttleDelaySecs = spec.throttleDelaySecs() != null ? spec.throttleDelaySecs() : 5;
        this.samplePeriodMs = spec.samplePeriodMs() != null ? spec.samplePeriodMs() : 1000L;
        this.sampleUpdateMs = spec.sampleUpdateMs() != null ? spec.sampleUpdateMs() : 1000L;
        this.overloadRatio = spec.overloadRatio() != null ? spec.overloadRatio() : 2.0;
        retryHandler = RetryHandler.withDefaults();
      }

      /** Instantiate the model handler and client. */
      @Setup
      public void setupHandler() {
        try {
          this.modelHandler = handlerClass.getDeclaredConstructor().newInstance();
          this.modelHandler.createClient(parameters);
          this.throttler =
              new ReactiveThrottler(
                  samplePeriodMs,
                  sampleUpdateMs,
                  overloadRatio,
                  "RemoteInference",
                  throttleDelaySecs);
        } catch (Exception e) {
          throw new RuntimeException("Failed to instantiate handler: " + handlerClass.getName(), e);
        }
      }
      /** Perform Inference. */
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Iterable<PredictionResult<InputT, OutputT>> response =
            retryHandler.execute(
                () -> {
                  if (throttler != null) {
                    throttler.throttle();
                  }
                  long reqTime = System.currentTimeMillis();
                  if (modelHandler == null) {
                    throw new IllegalStateException("modelHandler is not initialized");
                  }
                  Iterable<PredictionResult<InputT, OutputT>> result =
                      modelHandler.request(c.element());
                  if (throttler != null) {
                    throttler.successfulRequest(reqTime);
                  }
                  return result;
                });
        c.output(response);
      }
    }
  }
}
