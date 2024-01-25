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
package org.apache.beam.sdk.fn.test;

import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/** Utility methods which enable testing of {@link StreamObserver}s. */
public class TestStreams {
  /**
   * Creates a test {@link CallStreamObserver} {@link Builder} that forwards {@link
   * StreamObserver#onNext} calls to the supplied {@link Consumer}.
   */
  public static <T> Builder<T> withOnNext(Consumer<T> onNext) {
    return new Builder<>(
        new ForwardingCallStreamObserver<>(
            onNext,
            TestStreams.throwingErrorHandler(),
            TestStreams.noopRunnable(),
            TestStreams.alwaysTrueSupplier()));
  }

  /** A builder for a test {@link CallStreamObserver} that performs various callbacks. */
  public static class Builder<T> {
    private final ForwardingCallStreamObserver<T> observer;

    private Builder(ForwardingCallStreamObserver<T> observer) {
      this.observer = observer;
    }

    /**
     * Returns a new {@link Builder} like this one with the specified {@link
     * CallStreamObserver#isReady} callback.
     */
    public Builder<T> withIsReady(Supplier<Boolean> isReady) {
      return new Builder<>(
          new ForwardingCallStreamObserver<>(
              observer.onNext, observer.onError, observer.onCompleted, isReady));
    }

    /**
     * Returns a new {@link Builder} like this one with the specified {@link
     * StreamObserver#onCompleted} callback.
     */
    public Builder<T> withOnCompleted(Runnable onCompleted) {
      return new Builder<>(
          new ForwardingCallStreamObserver<>(
              observer.onNext, observer.onError, onCompleted, observer.isReady));
    }

    /**
     * Returns a new {@link Builder} like this one with the specified {@link StreamObserver#onError}
     * callback.
     */
    public Builder<T> withOnError(final Runnable onError) {
      return new Builder<>(
          new ForwardingCallStreamObserver<>(
              observer.onNext, t -> onError.run(), observer.onCompleted, observer.isReady));
    }

    /**
     * Returns a new {@link Builder} like this one with the specified {@link StreamObserver#onError}
     * consumer.
     */
    public Builder<T> withOnError(Consumer<Throwable> onError) {
      return new Builder<>(
          new ForwardingCallStreamObserver<>(
              observer.onNext, onError, observer.onCompleted, observer.isReady));
    }

    public CallStreamObserver<T> build() {
      return observer;
    }
  }

  private static Consumer<Throwable> throwingErrorHandler() {
    return item -> {
      throw new RuntimeException(item);
    };
  }

  private static Runnable noopRunnable() {
    return () -> {};
  }

  private static Supplier<Boolean> alwaysTrueSupplier() {
    return () -> true;
  }

  /** A {@link CallStreamObserver} which executes the supplied callbacks. */
  private static class ForwardingCallStreamObserver<T> extends CallStreamObserver<T> {
    private final Consumer<T> onNext;
    private final Supplier<Boolean> isReady;
    private final Consumer<Throwable> onError;
    private final Runnable onCompleted;

    public ForwardingCallStreamObserver(
        Consumer<T> onNext,
        Consumer<Throwable> onError,
        Runnable onCompleted,
        Supplier<Boolean> isReady) {
      this.onNext = onNext;
      this.onError = onError;
      this.onCompleted = onCompleted;
      this.isReady = isReady;
    }

    @Override
    public void onNext(T value) {
      onNext.accept(value);
    }

    @Override
    public void onError(Throwable t) {
      onError.accept(t);
    }

    @Override
    public void onCompleted() {
      onCompleted.run();
    }

    @Override
    public boolean isReady() {
      return isReady.get();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int count) {}

    @Override
    public void setMessageCompression(boolean enable) {}
  }
}
