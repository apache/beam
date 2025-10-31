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
import org.checkerframework.checker.nullness.qual.Nullable;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


import com.google.auto.value.AutoValue;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RemoteInference {

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

    abstract @Nullable BatchConfig batchConfig();

    abstract Builder<InputT, OutputT> builder();

    @AutoValue.Builder
    abstract static class Builder<InputT extends BaseInput, OutputT extends BaseResponse> {

      abstract Builder<InputT, OutputT> setHandler(Class<? extends BaseModelHandler> modelHandler);

      abstract Builder<InputT, OutputT> setParameters(BaseModelParameters modelParameters);

      abstract Builder<InputT, OutputT> setBatchConfig(BatchConfig config);

      abstract Invoke<InputT, OutputT> build();
    }

    public Invoke<InputT, OutputT> handler(Class<? extends BaseModelHandler> modelHandler) {
      return builder().setHandler(modelHandler).build();
    }

    public Invoke<InputT, OutputT> withParameters(BaseModelParameters modelParameters) {
      return builder().setParameters(modelParameters).build();
    }

    public Invoke<InputT, OutputT> withBatchConfig(BatchConfig config) {
      return builder().setBatchConfig(config).build();
    }

    @Override
    public PCollection<Iterable<PredictionResult<InputT, OutputT>>> expand(PCollection<InputT> input) {
      return input.apply(ParDo.of(new BatchElementsFn<>(this.batchConfig() != null ? this.batchConfig()
          : this
          .parameters()
          .defaultBatchConfig())))
        .apply(ParDo.of(new RemoteInferenceFn<>(this)));
    }

    static class RemoteInferenceFn<InputT extends BaseInput, OutputT extends BaseResponse>
      extends DoFn<List<InputT>, Iterable<PredictionResult<InputT, OutputT>>> {

      private final Class<? extends BaseModelHandler> handlerClass;
      private final BaseModelParameters parameters;
      private transient BaseModelHandler handler;

      RemoteInferenceFn(Invoke<InputT, OutputT> spec) {
        this.handlerClass = spec.handler();
        this.parameters = spec.parameters();
      }

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

      @ProcessElement
      public void processElement(ProcessContext c) {
        Iterable<PredictionResult<InputT, OutputT>> response = this.handler.request(c.element());
        c.output(response);
      }
    }

    public static class BatchElementsFn<T> extends DoFn<T, List<T>> {
      private final BatchConfig config;
      private List<T> batch;

      public BatchElementsFn(BatchConfig config) {
        this.config = config;
      }

      @StartBundle
      public void startBundle() {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        batch.add(c.element());
        if (batch.size() >= config.getMaxBatchSize()) {
          c.output(new ArrayList<>(batch));
          batch.clear();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) {
        if (!batch.isEmpty()) {
          c.output(new ArrayList<>(batch), null, null);
          batch.clear();
        }
      }

    }
  }
}
