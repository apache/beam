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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResettableStreamObserverTest {
  private final StreamObserver<Integer> delegate = newDelegate();

  private static StreamObserver<Integer> newDelegate() {
    return spy(
        new StreamObserver<Integer>() {
          @Override
          public void onNext(Integer integer) {}

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        });
  }

  @Test
  public void testPoison_beforeDelegateSet() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.poison();
    verifyNoInteractions(delegate);
  }

  @Test
  public void testPoison_afterDelegateSet() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.reset();
    observer.poison();
    verify(delegate).onError(isA(WindmillStreamShutdownException.class));
  }

  @Test
  public void testReset_afterPoisonedThrows() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.poison();
    assertThrows(WindmillStreamShutdownException.class, observer::reset);
  }

  @Test
  public void testOnNext_afterPoisonedThrows() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.poison();
    assertThrows(WindmillStreamShutdownException.class, () -> observer.onNext(1));
  }

  @Test
  public void testOnError_afterPoisonedThrows() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.poison();
    assertThrows(
        WindmillStreamShutdownException.class,
        () -> observer.onError(new RuntimeException("something bad happened.")));
  }

  @Test
  public void testOnCompleted_afterPoisonedThrows() {
    ResettableStreamObserver<Integer> observer = new ResettableStreamObserver<>(() -> delegate);
    observer.poison();
    assertThrows(WindmillStreamShutdownException.class, observer::onCompleted);
  }

  @Test
  public void testReset_usesNewDelegate() {
    List<StreamObserver<Integer>> delegates = new ArrayList<>();
    ResettableStreamObserver<Integer> observer =
        new ResettableStreamObserver<>(
            () -> {
              StreamObserver<Integer> delegate = newDelegate();
              delegates.add(delegate);
              return delegate;
            });
    observer.reset();
    observer.onNext(1);
    observer.reset();
    observer.onNext(2);

    StreamObserver<Integer> firstObserver = delegates.get(0);
    StreamObserver<Integer> secondObserver = delegates.get(1);

    verify(firstObserver).onNext(eq(1));
    verify(secondObserver).onNext(eq(2));
  }
}
