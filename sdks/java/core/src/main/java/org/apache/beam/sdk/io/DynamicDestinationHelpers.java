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
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Config;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.ConfigCoder;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;

/**
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
      return VoidCoder.of();
    }

    @Override
    public Void getDefaultDestination() {
      return (Void) null;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Void destination) {
      return filenamePolicy;
    }
  }

  public abstract static class DefaultDynamicDestinations<T>
      extends DynamicDestinations<T, DefaultFilenamePolicy.Config> {
    @Nullable
    @Override
    public Coder getDestinationCoder() {
      return ConfigCoder.of();
    }

    @Override
    public FilenamePolicy getFilenamePolicy(DefaultFilenamePolicy.Config config) {
      return DefaultFilenamePolicy.fromConfig(config);
    }
  }

  public static class ConstantDefaultDynamicDestinations<T> extends DefaultDynamicDestinations<T> {
    private Config config;

    public ConstantDefaultDynamicDestinations(Config config) {
      this.config = config;
    }

    @Override
    public Config getDestination(T element) {
      return config;
    }

    @Override
    public Config getDefaultDestination() {
      return config;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Config config) {
      return super.getFilenamePolicy(config);
    }
  }
}
