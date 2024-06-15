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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DirectStreamObserverTest {
  private final Set<Integer> sentOnStream = new HashSet<>();

  private DirectStreamObserver<Integer> newStreamObserver(long deadlineSeconds) {
    return new DirectStreamObserver<>(
        new AdvancingPhaser(1),
        new CallStreamObserver<Integer>() {
          @Override
          public boolean isReady() {
            return false;
          }

          @Override
          public void setOnReadyHandler(Runnable runnable) {}

          @Override
          public void disableAutoInboundFlowControl() {}

          @Override
          public void request(int i) {}

          @Override
          public void setMessageCompression(boolean b) {}

          @Override
          public void onNext(Integer integer) {
            sentOnStream.add(integer);
          }

          @Override
          public void onError(Throwable throwable) {}

          @Override
          public void onCompleted() {}
        },
        deadlineSeconds,
        1);
  }

  @Test
  public void testOnCompleted_shutsdownStreamObserverGracefully() throws InterruptedException {
    DirectStreamObserver<Integer> streamObserver = newStreamObserver(30);
    streamObserver.onNext(1);
    Thread onNextWaiting = new Thread(() -> streamObserver.onNext(10));
    onNextWaiting.start();
    streamObserver.onCompleted();
    onNextWaiting.join();
    assertFalse(sentOnStream.contains(10));
    assertThat(sentOnStream).containsExactly(1);
  }

  @Test
  public void testOnNext_timeoutThrowsStreamObserverCancelledException() {
    DirectStreamObserver<Integer> streamObserver = newStreamObserver(5);
    streamObserver.onNext(1);
    assertThrows(StreamObserverCancelledException.class, () -> streamObserver.onNext(10));
    assertFalse(sentOnStream.contains(10));
    assertThat(sentOnStream).containsExactly(1);
  }

  @Test
  public void testOnNext_interruptedThrowsStreamObserverCancelledException() {
    DirectStreamObserver<Integer> streamObserver = newStreamObserver(30);
    streamObserver.onNext(1);
    Thread onNextWaiting =
        new Thread(
            () ->
                assertThrows(
                    StreamObserverCancelledException.class, () -> streamObserver.onNext(10)));
    onNextWaiting.start();
    onNextWaiting.interrupt();
    assertFalse(sentOnStream.contains(10));
    assertThat(sentOnStream).containsExactly(1);
  }
}
