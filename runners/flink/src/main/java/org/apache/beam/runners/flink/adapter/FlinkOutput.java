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
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * A Beam PTransform used for marking an output that should be passed back to Flink.
 *
 * @param <T>
 */
/*package private*/ class FlinkOutput<T> extends PTransform<PCollection<T>, PDone> {

  public static final String URN = "beam:flink:internal:translation_output";

  private final String identifier;

  FlinkOutput(String identifier) {
    this.identifier = identifier;
  }

  @Override
  public PDone expand(PCollection<T> input) {
    return PDone.in(input.getPipeline());
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Translator
      implements PTransformTranslation.TransformPayloadTranslator<FlinkOutput<?>>,
          TransformPayloadTranslatorRegistrar {
    @Override
    public String getUrn() {
      return FlinkOutput.URN;
    }

    @Override
    @SuppressWarnings("nullness") // TODO(https://github.com/apache/beam/issues/20497)
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, FlinkOutput<?>> application, SdkComponents components)
        throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(FlinkOutput.URN)
          .setPayload(ByteString.copyFromUtf8(application.getTransform().identifier))
          .build();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(FlinkOutput.class, this);
    }
  }

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class FlinkInputOutputIsNativeTransform
      implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FlinkOutput.URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }
}
