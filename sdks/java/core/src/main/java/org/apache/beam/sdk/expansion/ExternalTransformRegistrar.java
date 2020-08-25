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
package org.apache.beam.sdk.expansion;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A registrar which contains a mapping from URNs to available {@link ExternalTransformBuilder}s.
 * Should be used with {@link com.google.auto.service.AutoService}.
 */
@Experimental(Kind.PORTABILITY)
public interface ExternalTransformRegistrar {

  /**
   * A mapping from URN to an {@link ExternalTransformBuilder} class.
   *
   * @deprecated Prefer implementing 'knownBuilderInstances'. This method will be removed in a
   *     future version of Beam.
   */
  @Deprecated
  default Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>>builder()
        .build();
  }

  /** A mapping from URN to an {@link ExternalTransformBuilder} instance. */
  default Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    ImmutableMap.Builder builder = ImmutableMap.<String, ExternalTransformBuilder>builder();
    Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders = knownBuilders();
    for (Entry<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilder :
        knownBuilders.entrySet()) {
      Preconditions.checkState(
          ExternalTransformBuilder.class.isAssignableFrom(knownBuilder.getValue()),
          "Provided identifier %s is not an ExternalTransformBuilder.",
          knownBuilder.getValue().getName());
      try {
        Constructor<? extends ExternalTransformBuilder> constructor =
            knownBuilder.getValue().getDeclaredConstructor();

        constructor.setAccessible(true);
        builder.put(knownBuilder.getKey(), constructor.newInstance());

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to instantiate ExternalTransformBuilder from constructor.");
      }
    }
    return builder.build();
  }
}
