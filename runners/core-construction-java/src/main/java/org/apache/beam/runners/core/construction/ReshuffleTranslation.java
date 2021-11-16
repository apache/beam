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
package org.apache.beam.runners.core.construction;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Utility methods for translating a {@link Reshuffle} to and from {@link RunnerApi}
 * representations.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class ReshuffleTranslation {

  static class ReshuffleTranslator implements TransformPayloadTranslator<Reshuffle<?, ?>> {
    @Override
    public String getUrn(Reshuffle<?, ?> transform) {
      return PTransformTranslation.RESHUFFLE_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Reshuffle<?, ?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder().setUrn(getUrn(transform.getTransform())).build();
    }
  }

  static class ReshufflePerKeyTranslator
      implements TransformPayloadTranslator<Reshuffle.Keys<?, ?>> {
    @Override
    public String getUrn(Reshuffle.Keys<?, ?> transform) {
      return PTransformTranslation.RESHUFFLE_KEYS_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Reshuffle.Keys<?, ?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder().setUrn(getUrn(transform.getTransform())).build();
    }
  }

  static class ReshufflePerRandomKeyTranslator
      implements TransformPayloadTranslator<Reshuffle.Elements<?>> {
    @Override
    public String getUrn(Reshuffle.Elements<?> transform) {
      return PTransformTranslation.RESHUFFLE_ELEMENTS_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, Reshuffle.Elements<?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder().setUrn(getUrn(transform.getTransform())).build();
    }
  }

  /**
   * Registers {@link ReshuffleTranslator}, {@link ReshufflePerKeyTranslator} and {@link
   * ReshufflePerRandomKeyTranslator}.
   */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.of(
          Reshuffle.class, new ReshuffleTranslator(),
          Reshuffle.Keys.class, new ReshufflePerKeyTranslator(),
          Reshuffle.Elements.class, new ReshufflePerRandomKeyTranslator());
    }
  }
}
