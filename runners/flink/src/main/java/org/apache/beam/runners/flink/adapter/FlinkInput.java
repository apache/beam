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
package org.apache.beam.runners.flink.adapter;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * A Beam PTransform used for marking an input that comes from Flink.
 *
 * @param <T>
 */
/*package private*/ class FlinkInput<T> extends PTransform<PBegin, PCollection<T>> {
  public static final String URN = "beam:flink:internal:translation_input";

  private final String identifier;

  private final Coder<T> coder;

  private final boolean isBounded;

  FlinkInput(String identifier, Coder<T> coder, boolean isBounded) {
    this.identifier = identifier;
    this.coder = coder;
    this.isBounded = isBounded;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        WindowingStrategy.globalDefault(),
        isBounded ? PCollection.IsBounded.BOUNDED : PCollection.IsBounded.UNBOUNDED,
        coder);
  }

  // This Translator translates this kind of PTransform into a Beam proto representation.
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Translator
      implements PTransformTranslation.TransformPayloadTranslator<FlinkInput<?>>,
          TransformPayloadTranslatorRegistrar {
    @Override
    public String getUrn() {
      return URN;
    }

    @Override
    @SuppressWarnings("nullness") // TODO(https://github.com/apache/beam/issues/20497)
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, FlinkInput<?>> application, SdkComponents components)
        throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(FlinkInput.URN)
          .setPayload(ByteString.copyFromUtf8(application.getTransform().identifier))
          .build();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(FlinkInput.class, this);
    }
  }

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class FlinkInputOutputIsNativeTransform
      implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FlinkInput.URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }
}
