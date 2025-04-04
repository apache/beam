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
package org.apache.beam.sdk.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.opentelemetry.api.trace.TracerProvider;
import java.util.function.Function;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface OpenTelemetryTracingOptions extends PipelineOptions {

  @Description("Enable OpenTelemetry tracing in SDK and supported IOs")
  @Default.Boolean(false)
  boolean getEnableOpenTelemetryTracing();

  void setEnableOpenTelemetryTracing(String enableOpenTelemetryTracing);

  @Description("OpenTelemetry resource service name.")
  String getServiceName();

  void setServiceName(String serviceName);

  @Description(
      "Tracer provider to inject to components of the pipeline e.g. ParDos or IOs supporting OpenTelemetry tracing. Some runners may provide built-in default provider.")
  @JsonIgnore
  @Nullable
  TracerProvider getTracerProvider();

  /**
   * Used to setting dynamically by Harness during worker startup.
   *
   * @param tracerProvider tracer provider
   */
  @Hidden
  void setTracerProvider(TracerProvider tracerProvider);

  @Description(
      "Tracer provider factory class that will setup OpenTelemetry SDK and provide tracerProvider object in return. Factory is invoked on every runner during startup. Runners may provide default built-in factory.")
  Class<? extends Function<OpenTelemetryTracingOptions, TracerProvider>> getTracerProviderFactory();

  void setTracerProviderFactory(
      Class<? extends Function<OpenTelemetryTracingOptions, TracerProvider>> factory);
}
