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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * Utility methods for translating a {@link View} transforms to and from {@link RunnerApi}
 * representations.
 *
 * @deprecated this should generally be done as part of {@link ParDo} translation, or moved into a
 *     dedicated runners-core-construction auxiliary class
 */
@Deprecated
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class CreatePCollectionViewTranslation {

  /**
   * @deprecated Since {@link CreatePCollectionView} is not a part of the Beam model, there is no
   *     SDK-agnostic specification. Using this method means your runner is tied to Java.
   */
  @Deprecated
  public static <ElemT, ViewT> PCollectionView<ViewT> getView(
      AppliedPTransform<
              PCollection<ElemT>,
              PCollection<ElemT>,
              PTransform<PCollection<ElemT>, PCollection<ElemT>>>
          application)
      throws IOException {

    RunnerApi.PTransform transformProto =
        PTransformTranslation.toProto(
            application,
            Collections.emptyList(),
            SdkComponents.create(application.getPipeline().getOptions()));

    checkArgument(
        PTransformTranslation.CREATE_VIEW_TRANSFORM_URN.equals(transformProto.getSpec().getUrn()),
        "Illegal attempt to extract %s from transform %s with name \"%s\" and URN \"%s\"",
        PCollectionView.class.getSimpleName(),
        application.getTransform(),
        application.getFullName(),
        transformProto.getSpec().getUrn());

    return (PCollectionView<ViewT>)
        SerializableUtils.deserializeFromByteArray(
            transformProto.getSpec().getPayload().toByteArray(),
            PCollectionView.class.getSimpleName());
  }

  /**
   * @deprecated runners should move away from translating `CreatePCollectionView` and treat this as
   *     part of the translation for a `ParDo` side input.
   */
  @Deprecated
  static class CreatePCollectionViewTranslator
      implements TransformPayloadTranslator<View.CreatePCollectionView<?, ?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.CREATE_VIEW_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, View.CreatePCollectionView<?, ?>> transform,
        SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(
              ByteString.copyFrom(
                  SerializableUtils.serializeToByteArray(transform.getTransform().getView())))
          .build();
    }
  }

  /**
   * Registers {@link CreatePCollectionViewTranslator}.
   *
   * @deprecated runners should move away from translating `CreatePCollectionView` and treat this as
   *     part of the translation for a `ParDo` side input.
   */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  @Deprecated
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(
          View.CreatePCollectionView.class, new CreatePCollectionViewTranslator());
    }
  }
}
