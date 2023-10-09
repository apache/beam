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
package org.apache.beam.sdk.schemas.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Internal;

/** Helpers for implementing the "Provider" pattern. */
@Internal
public final class Providers {

  public interface Identifyable {
    /**
     * Returns an id that uniquely represents this among others implementing its derived interface.
     */
    String identifier();
  }

  private Providers() {}

  public static <T extends Identifyable> Map<String, T> loadProviders(Class<T> klass) {
    Map<String, T> providers = new HashMap<>();
    for (T provider : ServiceLoader.load(klass)) {
      // Avro provider is treated as a special case since two Avro providers may want to be loaded -
      // from "core" (deprecated) and from "extensions/avro" (actual) - but only one must succeed.
      // TODO: we won't need this check once all Avro providers from "core" will be
      // removed
      if (provider.identifier().equals("avro")) {
        // Avro provider from "extensions/avro" must have a priority.
        if (provider.getClass().getName().startsWith("org.apache.beam.sdk.extensions.avro")) {
          // Load Avro provider from "extensions/avro" by any case.
          providers.put(provider.identifier(), provider);
        } else {
          // Load Avro provider from "core" if it was not loaded from Avro extension before.
          providers.putIfAbsent(provider.identifier(), provider);
        }
      } else {
        checkState(
            !providers.containsKey(provider.identifier()),
            "Duplicate providers exist with identifier `%s` for class %s.",
            provider.identifier(),
            klass);
        providers.put(provider.identifier(), provider);
      }
    }
    return providers;
  }
}
