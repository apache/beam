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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.ParamsCoder;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Some helper classes that derive from {@link FileBasedSink.DynamicDestinations}. */
public class DynamicFileDestinations {
  /** Always returns a constant {@link FilenamePolicy}. */
  private static class ConstantFilenamePolicy<UserT, OutputT>
      extends DynamicDestinations<UserT, Void, OutputT> {
    private final FilenamePolicy filenamePolicy;
    private final SerializableFunction<UserT, OutputT> formatFunction;

    public ConstantFilenamePolicy(
        FilenamePolicy filenamePolicy, SerializableFunction<UserT, OutputT> formatFunction) {
      this.filenamePolicy = filenamePolicy;
      this.formatFunction = formatFunction;
    }

    @Override
    public OutputT formatRecord(UserT record) {
      return formatFunction.apply(record);
    }

    @Override
    public @Nullable Void getDestination(UserT element) {
      return (Void) null;
    }

    @Override
    public @Nullable Coder<Void> getDestinationCoder() {
      return null;
    }

    @Override
    public @Nullable Void getDefaultDestination() {
      return (Void) null;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Void destination) {
      return filenamePolicy;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      checkState(filenamePolicy != null);
      filenamePolicy.populateDisplayData(builder);
    }
  }

  /**
   * A base class for a {@link DynamicDestinations} object that returns differently-configured
   * instances of {@link DefaultFilenamePolicy}.
   */
  private static class DefaultPolicyDestinations<UserT, OutputT>
      extends DynamicDestinations<UserT, Params, OutputT> {
    private final SerializableFunction<UserT, Params> destinationFunction;
    private final Params emptyDestination;
    private final SerializableFunction<UserT, OutputT> formatFunction;

    public DefaultPolicyDestinations(
        SerializableFunction<UserT, Params> destinationFunction,
        Params emptyDestination,
        SerializableFunction<UserT, OutputT> formatFunction) {
      this.destinationFunction = destinationFunction;
      this.emptyDestination = emptyDestination;
      this.formatFunction = formatFunction;
    }

    @Override
    public OutputT formatRecord(UserT record) {
      return formatFunction.apply(record);
    }

    @Override
    public Params getDestination(UserT element) {
      return destinationFunction.apply(element);
    }

    @Override
    public Params getDefaultDestination() {
      return emptyDestination;
    }

    @Nullable
    @Override
    public Coder<Params> getDestinationCoder() {
      return ParamsCoder.of();
    }

    @Override
    public FilenamePolicy getFilenamePolicy(DefaultFilenamePolicy.Params params) {
      return DefaultFilenamePolicy.fromParams(params);
    }
  }

  /** Returns a {@link DynamicDestinations} that always returns the same {@link FilenamePolicy}. */
  public static <UserT, OutputT> DynamicDestinations<UserT, Void, OutputT> constant(
      FilenamePolicy filenamePolicy, SerializableFunction<UserT, OutputT> formatFunction) {
    return new ConstantFilenamePolicy<>(filenamePolicy, formatFunction);
  }

  /**
   * A specialization of {@link #constant(FilenamePolicy, SerializableFunction)} for the case where
   * UserT and OutputT are the same type and the format function is the identity.
   */
  public static <UserT> DynamicDestinations<UserT, Void, UserT> constant(
      FilenamePolicy filenamePolicy) {
    return new ConstantFilenamePolicy<>(filenamePolicy, SerializableFunctions.<UserT>identity());
  }

  /**
   * Returns a {@link DynamicDestinations} that returns instances of {@link DefaultFilenamePolicy}
   * configured with the given {@link Params}.
   */
  public static <UserT, OutputT> DynamicDestinations<UserT, Params, OutputT> toDefaultPolicies(
      SerializableFunction<UserT, Params> destinationFunction,
      Params emptyDestination,
      SerializableFunction<UserT, OutputT> formatFunction) {
    return new DefaultPolicyDestinations<>(destinationFunction, emptyDestination, formatFunction);
  }
}
