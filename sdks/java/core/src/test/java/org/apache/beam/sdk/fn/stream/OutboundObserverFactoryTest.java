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
package org.apache.beam.sdk.fn.stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link OutboundObserverFactory}. */
@RunWith(JUnit4.class)
public class OutboundObserverFactoryTest {
  @Mock private StreamObserver<Integer> mockRequestObserver;
  @Mock private CallStreamObserver<String> mockResponseObserver;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testDefaultInstantiation() {
    StreamObserver<String> observer =
        OutboundObserverFactory.clientDirect()
            .outboundObserverFor(this.fakeFactory(), mockRequestObserver);
    assertThat(observer, instanceOf(DirectStreamObserver.class));
  }

  @Test
  public void testBufferedStreamInstantiation() {
    StreamObserver<String> observer =
        OutboundObserverFactory.clientBuffered(Executors.newSingleThreadExecutor())
            .outboundObserverFor(this.fakeFactory(), mockRequestObserver);
    assertThat(observer, instanceOf(BufferingStreamObserver.class));
  }

  @Test
  public void testBufferedStreamWithLimitInstantiation() {
    StreamObserver<String> observer =
        OutboundObserverFactory.clientBuffered(Executors.newSingleThreadExecutor(), 1)
            .outboundObserverFor(this.fakeFactory(), mockRequestObserver);
    assertThat(observer, instanceOf(BufferingStreamObserver.class));
    assertEquals(1, ((BufferingStreamObserver<String>) observer).getBufferSize());
  }

  private OutboundObserverFactory.BasicFactory<Integer, String> fakeFactory() {
    return inboundObserver -> {
      assertThat(inboundObserver, instanceOf(ForwardingClientResponseObserver.class));
      return mockResponseObserver;
    };
  }
}
