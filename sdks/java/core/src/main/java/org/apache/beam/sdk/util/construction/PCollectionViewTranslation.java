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

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;

/** Utilities for interacting with PCollection view protos. */
public class PCollectionViewTranslation {

  /**
   * Create a {@link PCollectionView} from a side input spec and an already-deserialized {@link
   * PCollection} that should be wired up.
   */
  public static PCollectionView<?> viewFromProto(
      RunnerApi.SideInput sideInput,
      String localName,
      PCollection<?> pCollection,
      RunnerApi.PTransform parDoTransform,
      RehydratedComponents components)
      throws IOException {
    checkArgument(
        localName != null,
        "%s.viewFromProto: localName must not be null",
        ParDoTranslation.class.getSimpleName());
    TupleTag<?> tag = new TupleTag<>(localName);
    WindowMappingFn<?> windowMappingFn = windowMappingFnFromProto(sideInput.getWindowMappingFn());
    ViewFn<?, ?> viewFn = viewFnFromProto(sideInput.getViewFn());
    WindowingStrategy<?, ?> windowingStrategy = pCollection.getWindowingStrategy().fixDefaults();

    PCollectionView<?> view =
        new RunnerPCollectionView<>(
            pCollection,
            (TupleTag) tag,
            (ViewFn) viewFn,
            windowMappingFn,
            windowingStrategy,
            (Coder) pCollection.getCoder());
    return view;
  }

  /**
   * Converts a {@link org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec} into a {@link
   * ViewFn} using the URN.
   */
  public static ViewFn<?, ?> viewFnFromProto(RunnerApi.FunctionSpec viewFn)
      throws InvalidProtocolBufferException {
    RunnerApi.FunctionSpec spec = viewFn;
    checkArgument(
        spec.getUrn().equals(ParDoTranslation.CUSTOM_JAVA_VIEW_FN_URN),
        "Can't deserialize unknown %s type %s",
        ViewFn.class.getSimpleName(),
        spec.getUrn());
    return (ViewFn<?, ?>)
        SerializableUtils.deserializeFromByteArray(
            spec.getPayload().toByteArray(), "Custom ViewFn");
  }

  /**
   * Converts a {@link org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec} into a {@link
   * WindowMappingFn} using the URN.
   */
  public static WindowMappingFn<?> windowMappingFnFromProto(RunnerApi.FunctionSpec windowMappingFn)
      throws InvalidProtocolBufferException {
    RunnerApi.FunctionSpec spec = windowMappingFn;
    checkArgument(
        spec.getUrn().equals(ParDoTranslation.CUSTOM_JAVA_WINDOW_MAPPING_FN_URN),
        "Can't deserialize unknown %s type %s",
        WindowMappingFn.class.getSimpleName(),
        spec.getUrn());
    return (WindowMappingFn<?>)
        SerializableUtils.deserializeFromByteArray(
            spec.getPayload().toByteArray(), "Custom WinodwMappingFn");
  }
}
