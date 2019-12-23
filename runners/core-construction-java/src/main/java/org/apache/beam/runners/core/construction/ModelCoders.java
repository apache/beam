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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardCoders;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Utilities and constants ot interact with coders that are part of the Beam Model. */
public class ModelCoders {
  private ModelCoders() {}

  public static final String BYTES_CODER_URN = getUrn(StandardCoders.Enum.BYTES);
  public static final String BOOL_CODER_URN = getUrn(StandardCoders.Enum.BOOL);
  // Where is this required explicitly, instead of implicit within WindowedValue and LengthPrefix
  // coders?
  public static final String INT64_CODER_URN = getUrn(StandardCoders.Enum.VARINT);
  public static final String STRING_UTF8_CODER_URN = getUrn(StandardCoders.Enum.STRING_UTF8);

  public static final String DOUBLE_CODER_URN = getUrn(StandardCoders.Enum.DOUBLE);

  public static final String ITERABLE_CODER_URN = getUrn(StandardCoders.Enum.ITERABLE);
  public static final String TIMER_CODER_URN = getUrn(StandardCoders.Enum.TIMER);
  public static final String KV_CODER_URN = getUrn(StandardCoders.Enum.KV);
  public static final String LENGTH_PREFIX_CODER_URN = getUrn(StandardCoders.Enum.LENGTH_PREFIX);

  public static final String GLOBAL_WINDOW_CODER_URN = getUrn(StandardCoders.Enum.GLOBAL_WINDOW);
  // This isn't strictly required once there's a way to represent an 'unknown window' (i.e. the
  // custom window encoding + the maximum timestamp of the window, this can be used for interval
  // windows.
  public static final String INTERVAL_WINDOW_CODER_URN =
      getUrn(StandardCoders.Enum.INTERVAL_WINDOW);

  public static final String WINDOWED_VALUE_CODER_URN = getUrn(StandardCoders.Enum.WINDOWED_VALUE);
  public static final String PARAM_WINDOWED_VALUE_CODER_URN =
      getUrn(StandardCoders.Enum.PARAM_WINDOWED_VALUE);

  public static final String ROW_CODER_URN = getUrn(StandardCoders.Enum.ROW);

  private static final Set<String> MODEL_CODER_URNS =
      ImmutableSet.of(
          BYTES_CODER_URN,
          BOOL_CODER_URN,
          INT64_CODER_URN,
          STRING_UTF8_CODER_URN,
          ITERABLE_CODER_URN,
          TIMER_CODER_URN,
          KV_CODER_URN,
          LENGTH_PREFIX_CODER_URN,
          GLOBAL_WINDOW_CODER_URN,
          INTERVAL_WINDOW_CODER_URN,
          WINDOWED_VALUE_CODER_URN,
          DOUBLE_CODER_URN,
          ROW_CODER_URN,
          PARAM_WINDOWED_VALUE_CODER_URN);

  public static Set<String> urns() {
    return MODEL_CODER_URNS;
  }

  public static WindowedValueCoderComponents getWindowedValueCoderComponents(Coder coder) {
    checkArgument(WINDOWED_VALUE_CODER_URN.equals(coder.getSpec().getUrn()));
    return new AutoValue_ModelCoders_WindowedValueCoderComponents(
        coder.getComponentCoderIds(0), coder.getComponentCoderIds(1));
  }

  public static Coder windowedValueCoder(String elementCoderId, String windowCoderId) {
    return Coder.newBuilder()
        .setSpec(FunctionSpec.newBuilder().setUrn(WINDOWED_VALUE_CODER_URN))
        .addComponentCoderIds(elementCoderId)
        .addComponentCoderIds(windowCoderId)
        .build();
  }

  public static Coder paramWindowedValueCoder(
      String elementCoderId, String windowCoderId, byte[] payload) {
    return Coder.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(PARAM_WINDOWED_VALUE_CODER_URN)
                .setPayload(ByteString.copyFrom(payload)))
        .addComponentCoderIds(elementCoderId)
        .addComponentCoderIds(windowCoderId)
        .build();
  }

  /** Components of a Windowed Value {@link Coder} with names. */
  @AutoValue
  public abstract static class WindowedValueCoderComponents {
    public abstract String elementCoderId();

    public abstract String windowCoderId();
  }

  public static KvCoderComponents getKvCoderComponents(Coder coder) {
    checkArgument(KV_CODER_URN.equals(coder.getSpec().getUrn()));
    return new AutoValue_ModelCoders_KvCoderComponents(
        coder.getComponentCoderIds(0), coder.getComponentCoderIds(1));
  }

  public static Coder kvCoder(String keyCoderId, String valueCoderId) {
    return Coder.newBuilder()
        .setSpec(FunctionSpec.newBuilder().setUrn(KV_CODER_URN))
        .addComponentCoderIds(keyCoderId)
        .addComponentCoderIds(valueCoderId)
        .build();
  }

  /** Components of a KV {@link Coder} with names. */
  @AutoValue
  public abstract static class KvCoderComponents {
    public abstract String keyCoderId();

    public abstract String valueCoderId();
  }
}
