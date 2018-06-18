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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.GlobalWindowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Collector that outputs elements to {@link BeamCollector}.
 */
@NotThreadSafe
@Audience(Audience.Type.EXECUTOR)
public class DoFnCollector<InputT, OutputT, ElemT> implements Collector<ElemT>, Context,
    Serializable {

  private final AccumulatorProvider accumulators;
  private final BeamCollector<InputT, OutputT, ElemT> beamCollector;
  private transient DoFn<InputT, OutputT>.ProcessContext context;

  DoFnCollector(AccumulatorProvider accumulators,
      BeamCollector<InputT, OutputT, ElemT> beamCollector) {
    this.accumulators = accumulators;
    this.beamCollector = beamCollector;
  }

  @Override
  public void collect(ElemT elem) {
    beamCollector.collect(Objects.requireNonNull(context), elem);
  }

  @Override
  public Context asContext() {
    return this;
  }

  @Override
  public Window<?> getWindow() {
    // TODO: we need to return the element's window here
    return GlobalWindowing.Window.get();
  }

  @Override
  public Counter getCounter(String name) {
    return accumulators.getCounter(beamCollector.getOperatorName(), name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulators.getHistogram(beamCollector.getOperatorName(), name);
  }

  @Override
  public Timer getTimer(String name) {
    throw new UnsupportedOperationException("Timer not supported. Use histogram instead.");
  }

  void setProcessContext(DoFn<InputT, OutputT>.ProcessContext context) {
    this.context = context;
  }

  /**
   * Translation of {@link ReduceByKeyTranslator.Collector} collect to Beam's context output.
   * OperatorName serve as namespace for Beam's metrics.
   * @param <InputT>
   * @param <OutputT>
   * @param <ElemT>
   */
  public interface BeamCollector<InputT, OutputT, ElemT> extends Serializable {

    void collect(DoFn<InputT, OutputT>.ProcessContext ctx, ElemT elem);

    String getOperatorName();
  }
}
