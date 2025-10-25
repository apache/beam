package org.apache.beam.sdk.ml.remoteinference;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.apache.beam.sdk.ml.remoteinference.base.BaseInput;
import org.apache.beam.sdk.ml.remoteinference.base.BaseModelHandler;
import org.apache.beam.sdk.ml.remoteinference.base.BaseModelParameters;
import org.apache.beam.sdk.ml.remoteinference.base.BaseResponse;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


import com.google.auto.value.AutoValue;

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
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    abstract @Nullable Class<? extends BaseModelHandler> handler();

    abstract @Nullable BaseModelParameters parameters();

    abstract Builder<InputT, OutputT> builder();

    @AutoValue.Builder
    abstract static class Builder<InputT extends BaseInput, OutputT extends BaseResponse> {

      abstract Builder<InputT, OutputT> setHandler(Class<? extends BaseModelHandler> modelHandler);

      abstract Builder<InputT, OutputT> setParameters(BaseModelParameters modelParameters);

      abstract Invoke<InputT, OutputT> build();
    }

    public Invoke<InputT, OutputT> handler(Class<? extends BaseModelHandler> modelHandler) {
      return builder().setHandler(modelHandler).build();
    }

    public Invoke<InputT, OutputT> withParameters(BaseModelParameters modelParameters) {
      return builder().setParameters(modelParameters).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      return input.apply(ParDo.of(new RemoteInferenceFn<>(this)));
    }

    static class RemoteInferenceFn<InputT extends BaseInput, OutputT extends BaseResponse>
      extends DoFn<InputT, OutputT> {

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
        OutputT response = (OutputT) this.handler.request(c.element());
        c.output(response);
      }
    }
  }
}
