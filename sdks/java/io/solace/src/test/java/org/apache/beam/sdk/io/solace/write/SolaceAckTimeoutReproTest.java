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
package org.apache.beam.sdk.io.solace.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSendMultipleEntry;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.MessageProducer;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.apache.beam.sdk.io.solace.broker.PublishResultHandler;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.SolaceMessageProducer;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Real Beam writers, producer wrapper and ACK handler; only the JCSMP transport is faked. */
@RunWith(JUnit4.class)
public class SolaceAckTimeoutReproTest {
  @After
  public void tearDownSessions() {
    SolaceWriteSessionsHandler.closeAllSessions();
  }

  @Test(timeout = 30000)
  public void acknowledgedMessagesMustNotTimeOutWhenBundlesShareASession() throws Exception {
    UnboundedBatchedSolaceWriter a =
        new UnboundedBatchedSolaceWriter(
            r -> JCSMPFactory.onlyInstance().createTopic("test/ack-routing"),
            new TestFactory(),
            DeliveryMode.PERSISTENT,
            SubmissionMode.HIGHER_THROUGHPUT,
            6,
            false);
    UnboundedBatchedSolaceWriter b = SerializableUtils.clone(a);
    a.startBundle();
    // Advance the worker-local round-robin index. Bundle B reuses A's producer slot.
    for (int i = 0; i < 5; i++) {
      SerializableUtils.clone(a).startBundle();
    }
    b.startBundle();
    TestSession session = (TestSession) a.solaceSessionServiceWithProducer();
    assertSame(session, b.solaceSessionServiceWithProducer());
    a.addToCurrentBundle(record("A"));
    b.addToCurrentBundle(record("B"));
    List<String> resultsA = new ArrayList<>();
    List<String> resultsB = new ArrayList<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> finishingB =
        executor.submit(
            () -> {
              try {
                b.finishBundle(context(b, resultsB));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    try {
      assertTrue("B's ACK must arrive first", session.bAcked.await(5, TimeUnit.SECONDS));
      a.finishBundle(context(a, resultsA));
      session.aFinished.countDown();
      finishingB.get(15, TimeUnit.SECONDS);
      assertEquals(Collections.singletonList("A"), resultsA);
      assertEquals(Collections.singletonList("B"), resultsB);
    } finally {
      try {
        session.aFinished.countDown();
        finishingB.cancel(true);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      } finally {
        a.teardown();
        b.teardown();
      }
    }
  }

  @Test(timeout = 30000)
  public void streamingWriterMustNotTimeOutWhenBundlesShareASession() throws Exception {
    UnboundedStreamingSolaceWriter a =
        new UnboundedStreamingSolaceWriter(
            r -> JCSMPFactory.onlyInstance().createTopic("test/ack-routing"),
            new TestFactory(),
            DeliveryMode.PERSISTENT,
            SubmissionMode.HIGHER_THROUGHPUT,
            6,
            false);
    UnboundedStreamingSolaceWriter b = SerializableUtils.clone(a);
    a.startBundle();
    for (int i = 0; i < 5; i++) {
      SerializableUtils.clone(a).startBundle();
    }
    b.startBundle();
    TestSession session = (TestSession) a.solaceSessionServiceWithProducer();
    assertSame(session, b.solaceSessionServiceWithProducer());

    List<String> resultsA = new ArrayList<>();
    List<String> resultsB = new ArrayList<>();
    ValueState<Integer> dummyState = dummyValueState();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> runningB =
        executor.submit(
            () -> {
              b.processElement(KV.of(0, record("B")), Instant.EPOCH, dummyState);
              b.finishBundle(context(b, resultsB));
            });
    try {
      assertTrue("B's ACK must arrive first", session.bAcked.await(5, TimeUnit.SECONDS));
      a.processElement(KV.of(0, record("A")), Instant.EPOCH, dummyState);
      a.finishBundle(context(a, resultsA));
      session.aFinished.countDown();
      runningB.get(15, TimeUnit.SECONDS);
      assertEquals(Collections.singletonList("A"), resultsA);
      assertEquals(Collections.singletonList("B"), resultsB);
    } finally {
      try {
        session.aFinished.countDown();
        runningB.cancel(true);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      } finally {
        a.teardown();
        b.teardown();
      }
    }
  }

  @Test(timeout = 30000)
  public void duplicateMessageIdsInSameBundleMustWaitForAllAcks() throws Exception {
    UnboundedBatchedSolaceWriter writer =
        new UnboundedBatchedSolaceWriter(
            r -> JCSMPFactory.onlyInstance().createTopic("test/ack-routing"),
            new TestFactory(),
            DeliveryMode.PERSISTENT,
            SubmissionMode.HIGHER_THROUGHPUT,
            1,
            false);
    writer.startBundle();
    writer.addToCurrentBundle(record("dup"));
    writer.addToCurrentBundle(record("dup"));
    List<String> results = new ArrayList<>();
    try {
      writer.finishBundle(context(writer, results));
      assertEquals(Arrays.asList("dup", "dup"), results);
    } finally {
      writer.teardown();
    }
  }

  @Test(timeout = 30000)
  public void teardownOfOneDoFnInstanceDuringRebalanceMustNotCloseSharedSessions()
      throws Exception {
    UnboundedBatchedSolaceWriter a =
        new UnboundedBatchedSolaceWriter(
            r -> JCSMPFactory.onlyInstance().createTopic("test/rebalance"),
            new TestFactory(),
            DeliveryMode.PERSISTENT,
            SubmissionMode.HIGHER_THROUGHPUT,
            2,
            false);
    UnboundedBatchedSolaceWriter b = SerializableUtils.clone(a);
    a.startBundle();
    b.startBundle();
    TestSession sessionA = (TestSession) a.solaceSessionServiceWithProducer();
    TestSession sessionB = (TestSession) b.solaceSessionServiceWithProducer();

    b.addToCurrentBundle(record("B_REBALANCE"));
    List<String> resultsB = new ArrayList<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> finishingB =
        executor.submit(
            () -> {
              try {
                b.finishBundle(context(b, resultsB));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    try {
      assertTrue(
          "B must send its message before A tears down", sessionB.bSent.await(5, TimeUnit.SECONDS));
      // Simulate DoFn instance A being torn down during rebalancing (e.g.
      // WorkItemCancelledException
      // or BundleProcessor cache eviction) while B is waiting for its in-flight ACK.
      a.teardown();
      assertFalse("Tearing down one DoFn instance must not close session A", sessionA.closed.get());
      assertFalse("Tearing down one DoFn instance must not close session B", sessionB.closed.get());

      sessionB.aFinished.countDown();
      finishingB.get(15, TimeUnit.SECONDS);
      assertEquals(Collections.singletonList("B_REBALANCE"), resultsB);
    } finally {
      try {
        sessionB.aFinished.countDown();
        finishingB.cancel(true);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      } finally {
        b.teardown();
      }
    }
  }

  @SuppressWarnings("nullness")
  private static ValueState<Integer> dummyValueState() {
    return new ValueState<Integer>() {
      private Integer value;

      @Override
      public void write(Integer input) {
        this.value = input;
      }

      @Override
      public Integer read() {
        return value;
      }

      @Override
      public ValueState<Integer> readLater() {
        return this;
      }

      @Override
      public void clear() {
        this.value = null;
      }
    };
  }

  private static Solace.Record record(String id) {
    return Solace.Record.builder().setMessageId(id).setPayload(new byte[0]).build();
  }

  @SuppressWarnings("nullness")
  private static DoFn<KV<Integer, Solace.Record>, Solace.PublishResult>.FinishBundleContext context(
      UnboundedSolaceWriter writer, List<String> results) {
    DoFn<KV<Integer, Solace.Record>, Solace.PublishResult> fn = writer;
    return fn.new FinishBundleContext() {
      @Override
      public PipelineOptions getPipelineOptions() {
        return null;
      }

      @Override
      public void output(Solace.PublishResult value, Instant timestamp, BoundedWindow window) {
        results.add(value.getMessageId());
      }

      @Override
      public <T> void output(TupleTag<T> tag, T value, Instant timestamp, BoundedWindow window) {
        results.add(((Solace.PublishResult) value).getMessageId());
      }
    };
  }

  private static class TestFactory extends SessionServiceFactory {
    @Override
    public SessionService create() {
      return new TestSession();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof TestFactory;
    }

    @Override
    public int hashCode() {
      return TestFactory.class.hashCode();
    }
  }

  private static class TestSession extends SessionService {
    final PublishResultHandler handler = new PublishResultHandler();
    final CountDownLatch bSent = new CountDownLatch(1);
    final CountDownLatch bAcked = new CountDownLatch(1);
    final CountDownLatch aFinished = new CountDownLatch(1);
    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<String> ackedIds = new CopyOnWriteArrayList<>();
    final MessageProducer producer =
        new SolaceMessageProducer(
            (XMLMessageProducer)
                Proxy.newProxyInstance(
                    XMLMessageProducer.class.getClassLoader(),
                    new Class<?>[] {XMLMessageProducer.class},
                    (proxy, method, args) -> {
                      if (method.getName().equals("sendMultiple")) {
                        if (closed.get()) {
                          throw new JCSMPException("Session is closed");
                        }
                        JCSMPSendMultipleEntry[] entries = (JCSMPSendMultipleEntry[]) args[0];
                        int offset = (Integer) args[1];
                        int length = (Integer) args[2];
                        for (int i = offset; i < offset + length; i++) {
                          XMLMessage message = entries[i].getMessage();
                          String id = message.getApplicationMessageId();
                          if (id.equals("B_REBALANCE")) {
                            bSent.countDown();
                            assertTrue(aFinished.await(5, TimeUnit.SECONDS));
                            if (!closed.get()) {
                              handler.responseReceivedEx(message.getCorrelationKey());
                              ackedIds.add(id);
                            }
                          } else {
                            if (!closed.get()) {
                              handler.responseReceivedEx(message.getCorrelationKey());
                              ackedIds.add(id);
                            }
                            if (id.equals("B")) {
                              bAcked.countDown();
                              assertTrue(aFinished.await(5, TimeUnit.SECONDS));
                            }
                          }
                        }
                        return length;
                      }
                      if (method.getName().equals("send")) {
                        if (closed.get()) {
                          throw new JCSMPException("Session is closed");
                        }
                        XMLMessage message = (XMLMessage) args[0];
                        String id = message.getApplicationMessageId();
                        if (id.equals("B_REBALANCE")) {
                          bSent.countDown();
                          assertTrue(aFinished.await(5, TimeUnit.SECONDS));
                          if (!closed.get()) {
                            handler.responseReceivedEx(message.getCorrelationKey());
                            ackedIds.add(id);
                          }
                        } else {
                          if (!closed.get()) {
                            handler.responseReceivedEx(message.getCorrelationKey());
                            ackedIds.add(id);
                          }
                          if (id.equals("B")) {
                            bAcked.countDown();
                            assertTrue(aFinished.await(5, TimeUnit.SECONDS));
                          }
                        }
                        return null;
                      }
                      if (method.getName().equals("isClosed")) {
                        return closed.get();
                      }
                      if (method.getName().equals("close")) {
                        closed.set(true);
                        return null;
                      }
                      throw new UnsupportedOperationException(method.getName());
                    }));

    @Override
    public void connect() {}

    @Override
    public void close() {
      closed.set(true);
    }

    @Override
    public MessageReceiver getReceiver() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageProducer getInitializedProducer(SubmissionMode mode) {
      return producer;
    }

    @Override
    public JCSMPProperties getSessionProperties() {
      return new JCSMPProperties();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return this == other;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }
}
