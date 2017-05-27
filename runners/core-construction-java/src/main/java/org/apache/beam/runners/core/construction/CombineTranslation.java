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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.CombinePayload;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Methods for translating between {@link Combine.PerKey} {@link PTransform PTransforms} and {@link
 * RunnerApi.CombinePayload} protos.
 */
public class CombineTranslation {
  public static final String JAVA_SERIALIZED_COMBINE_FN_URN = "urn:beam:java:combinefn:v1";

  public static CombinePayload toProto(
      AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>> combine, SdkComponents sdkComponents)
      throws IOException {
    GlobalCombineFn<?, ?, ?> combineFn = combine.getTransform().getFn();
    try {
      Coder<?> accumulatorCoder = extractAccumulatorCoder(combineFn, (AppliedPTransform) combine);
      Map<String, SideInput> sideInputs = new HashMap<>();
      return RunnerApi.CombinePayload.newBuilder()
          .setAccumulatorCoderId(sdkComponents.registerCoder(accumulatorCoder))
          .putAllSideInputs(sideInputs)
          .setCombineFn(toProto(combineFn))
          .build();
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <K, InputT, AccumT> Coder<AccumT> extractAccumulatorCoder(
      GlobalCombineFn<InputT, AccumT, ?> combineFn,
      AppliedPTransform<PCollection<KV<K, InputT>>, ?, Combine.PerKey<K, InputT, ?>> transform)
      throws CannotProvideCoderException {
    KvCoder<K, InputT> inputCoder =
        (KvCoder<K, InputT>)
            ((PCollection<KV<K, InputT>>) Iterables.getOnlyElement(transform.getInputs().values()))
                .getCoder();
    return AppliedCombineFn.withInputCoder(
            combineFn,
            transform.getPipeline().getCoderRegistry(),
            inputCoder,
            transform.getTransform().getSideInputs(),
            ((PCollection<?>) Iterables.getOnlyElement(transform.getOutputs().values()))
                .getWindowingStrategy())
        .getAccumulatorCoder();
  }

  private static SdkFunctionSpec toProto(GlobalCombineFn<?, ?, ?> combineFn) {
    return SdkFunctionSpec.newBuilder()
        // TODO: Set Java SDK Environment URN
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(JAVA_SERIALIZED_COMBINE_FN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(combineFn)))
                            .build())))
        .build();
  }

  public static Coder<?> getAccumulatorCoder(
      CombinePayload payload, RunnerApi.Components components) throws IOException {
    String id = payload.getAccumulatorCoderId();
    return CoderTranslation.fromProto(components.getCodersOrThrow(id), components);
  }

  public static GlobalCombineFn<?, ?, ?> getCombineFn(CombinePayload payload)
      throws IOException {
    checkArgument(payload.getCombineFn().getSpec().getUrn().equals(JAVA_SERIALIZED_COMBINE_FN_URN));
    return (GlobalCombineFn<?, ?, ?>)
        SerializableUtils.deserializeFromByteArray(
            payload
                .getCombineFn()
                .getSpec()
                .getParameter()
                .unpack(BytesValue.class)
                .getValue()
                .toByteArray(),
            "CombineFn");
  }
}
