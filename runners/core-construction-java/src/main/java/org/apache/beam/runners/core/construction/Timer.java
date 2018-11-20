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
package org.apache.beam.runners.core.construction;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.joda.time.Instant;

/**
 * A timer consists of a timestamp and a corresponding user supplied payload.
 *
 * <p>Note that this is an implementation helper specifically intended for use during execution by
 * runners and the Java SDK harness. The API for pipeline authors is {@link
 * org.apache.beam.sdk.state.Timer}.
 */
@AutoValue
public abstract class Timer<T> {

  /** Returns a timer for the given timestamp with a {@code null} payload. */
  public static Timer<Void> of(Instant time) {
    return of(time, (Void) null);
  }

  /** Returns a timer for the given timestamp with a user specified payload. */
  public static <T> Timer<T> of(Instant timestamp, @Nullable T payload) {
    return new AutoValue_Timer(timestamp, payload);
  }

  /**
   * Returns the timestamp of when the timer is scheduled to fire.
   *
   * <p>The time is relative to the time domain defined in the {@link
   * org.apache.beam.model.pipeline.v1.RunnerApi.TimerSpec} that is associated with this timer.
   */
  public abstract Instant getTimestamp();

  /** A user supplied payload. */
  @Nullable
  public abstract T getPayload();

  /**
   * A {@link org.apache.beam.sdk.coders.Coder} for timers.
   *
   * <p>This coder is deterministic if the payload coder is deterministic.
   *
   * <p>This coder is inexpensive for size estimation of elements if the payload coder is
   * inexpensive for size estimation.
   */
  public static class Coder<T> extends StructuredCoder<Timer<T>> {

    public static <T> Coder of(org.apache.beam.sdk.coders.Coder<T> payloadCoder) {
      return new Coder(payloadCoder);
    }

    private final org.apache.beam.sdk.coders.Coder<T> payloadCoder;

    private Coder(org.apache.beam.sdk.coders.Coder<T> payloadCoder) {
      this.payloadCoder = payloadCoder;
    }

    @Override
    public void encode(Timer<T> timer, OutputStream outStream) throws CoderException, IOException {
      InstantCoder.of().encode(timer.getTimestamp(), outStream);
      payloadCoder.encode(timer.getPayload(), outStream);
    }

    @Override
    public Timer<T> decode(InputStream inStream) throws CoderException, IOException {
      Instant instant = InstantCoder.of().decode(inStream);
      T value = payloadCoder.decode(inStream);
      return Timer.of(instant, value);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Collections.singletonList(payloadCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "Payload coder must be deterministic", payloadCoder);
    }

    @Override
    public boolean consistentWithEquals() {
      return payloadCoder.consistentWithEquals();
    }

    @Override
    public Object structuralValue(Timer<T> value) {
      return Timer.of(value.getTimestamp(), payloadCoder.structuralValue(value.getPayload()));
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Timer<T> value) {
      return payloadCoder.isRegisterByteSizeObserverCheap(value.getPayload());
    }

    @Override
    public void registerByteSizeObserver(Timer<T> value, ElementByteSizeObserver observer)
        throws Exception {
      InstantCoder.of().registerByteSizeObserver(value.getTimestamp(), observer);
      payloadCoder.registerByteSizeObserver(value.getPayload(), observer);
    }
  }
}
