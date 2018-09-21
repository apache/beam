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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;

/** Instantiate accumulator provider on the first usage. Thus {@link Serializable}. */
class LazyAccumulatorProvider implements AccumulatorProvider, Serializable {

  private final Factory factory;

  private transient AccumulatorProvider accumulators;

  LazyAccumulatorProvider(Factory factory) {
    this.factory = requireNonNull(factory);
  }

  @Override
  public Counter getCounter(String name) {
    return getAccumulatorProvider().getCounter(name);
  }

  @Override
  public Counter getCounter(String namespace, String name) {
    return getAccumulatorProvider().getCounter(namespace, name);
  }

  @Override
  public Histogram getHistogram(String name) {
    return getAccumulatorProvider().getHistogram(name);
  }

  @Override
  public Histogram getHistogram(String namespace, String name) {
    return getAccumulatorProvider().getHistogram(namespace, name);
  }

  @Override
  public Timer getTimer(String name) {
    return getAccumulatorProvider().getTimer(name);
  }

  private AccumulatorProvider getAccumulatorProvider() {
    if (accumulators == null) {
      accumulators = factory.create();
    }
    return accumulators;
  }
}
