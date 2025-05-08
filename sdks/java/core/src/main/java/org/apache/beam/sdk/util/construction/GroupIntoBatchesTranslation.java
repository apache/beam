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

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.GroupIntoBatchesPayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class GroupIntoBatchesTranslation {
  /**
   * A translator registered to translate {@link GroupIntoBatches} objects to protobuf
   * representation.
   */
  static class GroupIntoBatchesTranslator
      implements PTransformTranslation.TransformPayloadTranslator<GroupIntoBatches<?, ?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.GROUP_INTO_BATCHES_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, GroupIntoBatches<?, ?>> transform, SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(
              getPayloadFromParameters(transform.getTransform().getBatchingParams()).toByteString())
          .build();
    }
  }

  /**
   * A translator registered to translate {@link GroupIntoBatches.WithShardedKey} objects to
   * protobuf representation.
   */
  static class ShardedGroupIntoBatchesTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          GroupIntoBatches<?, ?>.WithShardedKey> {
    @Override
    public String getUrn() {
      return PTransformTranslation.GROUP_INTO_BATCHES_WITH_SHARDED_KEY_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, GroupIntoBatches<?, ?>.WithShardedKey> transform,
        SdkComponents components) {
      return FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(
              getPayloadFromParameters(transform.getTransform().getBatchingParams()).toByteString())
          .build();
    }
  }

  private static <K, V> GroupIntoBatchesPayload getPayloadFromParameters(
      GroupIntoBatches.BatchingParams params) {
    return RunnerApi.GroupIntoBatchesPayload.newBuilder()
        .setBatchSize(params.getBatchSize())
        .setBatchSizeBytes(params.getBatchSizeBytes())
        .setMaxBufferingDurationMillis(params.getMaxBufferingDuration().getMillis())
        .build();
  }

  /** Registers {@link GroupIntoBatchesTranslator} and {@link ShardedGroupIntoBatchesTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(GroupIntoBatches.class, new GroupIntoBatchesTranslator())
          .put(GroupIntoBatches.WithShardedKey.class, new ShardedGroupIntoBatchesTranslator())
          .build();
    }
  }
}
