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
package org.apache.beam.sdk.extensions.euphoria.core.translate.collector;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link Collector} which forwards output elements through {@link
 * CollectorAdapter} to given {@link DoFn.ProcessContext}. The {@link DoFn.ProcessContext} needs to
 * be set by {@link AdaptableCollector#setProcessContext(DoFn.ProcessContext)} manually before use.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@NotThreadSafe
@Audience(Audience.Type.EXECUTOR)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class AdaptableCollector<InputT, OutputT, ElemT>
    implements Collector<ElemT>, Context, Serializable {

  private static final String UNSUPPORTED = "Accumulators are supported for named operators only.";

  private final AccumulatorProvider accumulators;
  private final CollectorAdapter<InputT, OutputT, ElemT> adapter;
  private final @Nullable String operatorName;
  private transient DoFn<InputT, OutputT>.ProcessContext context;

  public AdaptableCollector(
      AccumulatorProvider accumulators,
      @Nullable String operatorName,
      CollectorAdapter<InputT, OutputT, ElemT> adapter) {
    this.accumulators = accumulators;
    this.operatorName = operatorName;
    this.adapter = adapter;
  }

  @Override
  public void collect(ElemT elem) {
    adapter.collect(requireNonNull(context), elem);
  }

  @Override
  public Context asContext() {
    return this;
  }

  @Override
  public Counter getCounter(String name) {
    return accumulators.getCounter(requireNonNull(operatorName, UNSUPPORTED), name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulators.getHistogram(requireNonNull(operatorName, UNSUPPORTED), name);
  }

  @Override
  public Timer getTimer(String name) {
    throw new UnsupportedOperationException("Timer not supported. Use histogram instead.");
  }

  public void setProcessContext(DoFn<InputT, OutputT>.ProcessContext context) {
    this.context = requireNonNull(context);
  }
}
