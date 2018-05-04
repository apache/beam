/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;
import org.apache.beam.sdk.transforms.DoFn;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.Objects;

/**
 * Collector that outputs elements to {@link BeamCollector}.
 */
@NotThreadSafe
@Audience(Audience.Type.EXECUTOR)
public class DoFnCollector<IN, OUT, ELEM> implements Collector<ELEM>, Context, Serializable {

  public interface BeamCollector<IN, OUT, ELEM> extends Serializable {

    void collect(DoFn<IN, OUT>.ProcessContext ctx, ELEM elem);
  }

  private final AccumulatorProvider accumulators;

  private final BeamCollector<IN, OUT, ELEM> beamCollector;

  private transient DoFn<IN, OUT>.ProcessContext context;

  DoFnCollector(AccumulatorProvider accumulators, BeamCollector<IN, OUT, ELEM> beamCollector) {
    this.accumulators = accumulators;
    this.beamCollector = beamCollector;
  }

  @Override
  public void collect(ELEM elem) {
    beamCollector.collect(Objects.requireNonNull(context), elem);
  }

  @Override
  public Context asContext() {
    return this;
  }

  @Override
  public Window<?> getWindow() {
    // FIXME: we need to return the element's window here
    return GlobalWindowing.Window.get();
  }

  @Override
  public Counter getCounter(String name) {
    return accumulators.getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return accumulators.getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    return accumulators.getTimer(name);
  }

  void setProcessContext(DoFn<IN, OUT>.ProcessContext context) {
    this.context = context;
  }
}
