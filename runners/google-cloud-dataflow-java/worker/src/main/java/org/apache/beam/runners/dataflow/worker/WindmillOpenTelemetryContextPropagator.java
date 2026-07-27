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
package org.apache.beam.runners.dataflow.worker;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
public class WindmillOpenTelemetryContextPropagator {

  public static final String TRACEPARENT = "traceparent";
  public static final String TRACESTATE = "tracestate";
  private static final TextMapSetter<BeamFnApi.Elements.ElementMetadata.Builder> SETTER =
      (carrier, key, value) -> {
        if (carrier == null) {
          return;
        }
        if (TRACEPARENT.equals(key)) {
          carrier.setTraceparent(value);
        } else if (TRACESTATE.equals(key)) {
          carrier.setTracestate(value);
        }
      };

  private static final TextMapGetter<BeamFnApi.Elements.ElementMetadata> GETTER =
      new TextMapGetter<BeamFnApi.Elements.ElementMetadata>() {
        @Override
        public Iterable<String> keys(BeamFnApi.Elements.ElementMetadata carrier) {
          return Lists.newArrayList(TRACEPARENT, TRACESTATE);
        }

        @Override
        public @Nullable String get(
            BeamFnApi.Elements.@Nullable ElementMetadata carrier, String key) {
          if (carrier == null) {
            return null;
          }
          if (TRACEPARENT.equalsIgnoreCase(key)) {
            return carrier.getTraceparent();
          } else if (TRACESTATE.equalsIgnoreCase(key)) {
            return carrier.getTracestate();
          }
          return null;
        }
      };

  public static void set(Context from, BeamFnApi.Elements.ElementMetadata.Builder builder) {
    W3CTraceContextPropagator.getInstance().inject(from, builder, SETTER);
  }

  public static Context read(BeamFnApi.Elements.ElementMetadata from) {
    return W3CTraceContextPropagator.getInstance().extract(Context.root(), from, GETTER);
  }
}
