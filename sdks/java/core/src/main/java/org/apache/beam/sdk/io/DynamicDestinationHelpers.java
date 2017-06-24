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

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.ParamsCoder;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;

/**
 * Some helper classes that derive from {@link FileBasedSink.DynamicDestinations}.
 */
public class DynamicDestinationHelpers {
  /**
   * Always returns a constant {@link FilenamePolicy}.
   */
  public static class ConstantFilenamePolicy<T> extends DynamicDestinations<T, Void> {
    private final FilenamePolicy filenamePolicy;

    public ConstantFilenamePolicy(FilenamePolicy filenamePolicy) {
      this.filenamePolicy = filenamePolicy;
    }

    @Override
    public Void getDestination(T element) {
      return (Void) null;
    }

    @Override
    public Coder<Void> getDestinationCoder() {
      return null;
    }

    @Override
    public Void getDefaultDestination() {
      return (Void) null;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Void destination) {
      return filenamePolicy;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      filenamePolicy.populateDisplayData(builder);
    }
  }

  /**
   * A base class for a {@link DynamicDestinations} object that returns differently-configured
   * instances of {@link DefaultFilenamePolicy} based on the key.
   */
  public static class DefaultDynamicDestinations
  extends DynamicDestinations<KV<Params, String>, Params> {
    Params emptyDestination;

    public DefaultDynamicDestinations(Params emptyDestination) {
      this.emptyDestination = emptyDestination;
    }

    @Override
    public Params getDestination(KV<Params, String> element) {
      return element.getKey();
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
}
