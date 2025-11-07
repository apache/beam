/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.ml.remoteinference;

import org.apache.beam.sdk.ml.remoteinference.base.*;
import org.apache.beam.sdk.transforms.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import org.apache.beam.sdk.values.PCollection;
import com.google.auto.value.AutoValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link PTransform} for making remote inference calls to external machine learning services.
 *
 * <p>{@code RemoteInference} provides a framework for integrating remote ML model
 * inference into Apache Beam pipelines and handles the communication between pipelines
 * and external inference APIs.
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
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class RemoteInference {

  /** Invoke the model handler with model parameters */
  public static <InputT extends BaseInput, OutputT extends BaseResponse> Invoke<InputT, OutputT> invoke() {
    return new AutoValue_RemoteInference_Invoke.Builder<InputT, OutputT>().setParameters(null)
      .build();
  }

  private RemoteInference() {
  }

  @AutoValue
  public abstract static class Invoke<InputT extends BaseInput, OutputT extends BaseResponse>
    extends PTransform<PCollection<InputT>, PCollection<Iterable<PredictionResult<InputT, OutputT>>>> {

    abstract @Nullable Class<? extends BaseModelHandler> handler();

    abstract @Nullable BaseModelParameters parameters();


    abstract Builder<InputT, OutputT> builder();

    @AutoValue.Builder
    abstract static class Builder<InputT extends BaseInput, OutputT extends BaseResponse> {

      abstract Builder<InputT, OutputT> setHandler(Class<? extends BaseModelHandler> modelHandler);

      abstract Builder<InputT, OutputT> setParameters(BaseModelParameters modelParameters);


      abstract Invoke<InputT, OutputT> build();
    }

    /**
     * Model handler class for inference.
     */
    public Invoke<InputT, OutputT> handler(Class<? extends BaseModelHandler> modelHandler) {
      return builder().setHandler(modelHandler).build();
    }

    /**
     * Configures the parameters for model initialization.
     */
    public Invoke<InputT, OutputT> withParameters(BaseModelParameters modelParameters) {
      return builder().setParameters(modelParameters).build();
    }


    @Override
    public PCollection<Iterable<PredictionResult<InputT, OutputT>>> expand(PCollection<InputT> input) {
      checkArgument(handler() != null, "handler() is required");
      checkArgument(parameters() != null, "withParameters() is required");
      return input
        .apply("WrapInputInList", MapElements.via(new SimpleFunction<InputT, List<InputT>>() {
          @Override
          public List<InputT> apply(InputT element) {
            return Collections.singletonList(element);
          }
        }))
        // Pass the list to the inference function
        .apply("RemoteInference", ParDo.of(new RemoteInferenceFn<InputT, OutputT>(this)));
    }

    /**
     *  A {@link DoFn} that performs remote inference operation.
     *
     *       <p>This function manages the lifecycle of the model handler:
     *       <ul>
     *         <li>Instantiates the handler during {@link Setup}</li>
     *         <li>Initializes the remote client via {@link BaseModelHandler#createClient}</li>
     *         <li>Processes elements by calling {@link BaseModelHandler#request}</li>
     *       </ul>
     */
    static class RemoteInferenceFn<InputT extends BaseInput, OutputT extends BaseResponse>
      extends DoFn<List<InputT>, Iterable<PredictionResult<InputT, OutputT>>> {

      private final Class<? extends BaseModelHandler> handlerClass;
      private final BaseModelParameters parameters;
      private transient BaseModelHandler handler;

      RemoteInferenceFn(Invoke<InputT, OutputT> spec) {
        this.handlerClass = spec.handler();
        this.parameters = spec.parameters();
      }

      /** Instantiate the model handler and client*/
      @Setup
      public void setupHandler() {
        try {
          this.handler = handlerClass.getDeclaredConstructor().newInstance();
          this.handler.createClient(parameters);
        } catch (Exception e) {
          throw new RuntimeException("Failed to instantiate handler: "
            + handlerClass.getName(), e);
        }
      }
      /** Perform Inference */
      @ProcessElement
      public void processElement(ProcessContext c) {
        Iterable<PredictionResult<InputT, OutputT>> response = this.handler.request(c.element());
        c.output(response);
      }
    }

  }
}
