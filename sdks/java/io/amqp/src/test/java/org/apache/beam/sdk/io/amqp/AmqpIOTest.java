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
package org.apache.beam.sdk.io.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.Tracker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests on {@link AmqpIO}. */
@RunWith(JUnit4.class)
public class AmqpIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpIOTest.class);

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedAmqpBroker broker = new EmbeddedAmqpBroker();

  @Test
  public void testRead() throws Exception {
    PCollection<Message> output =
        pipeline.apply(
            AmqpIO.read()
                .withMaxNumRecords(100)
                .withAddresses(Collections.singletonList(broker.getQueueUri("testRead"))));
    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(100L);

    Messenger sender = Messenger.Factory.create();
    sender.start();
    for (int i = 0; i < 100; i++) {
      Message message = Message.Factory.create();
      message.setAddress(broker.getQueueUri("testRead"));
      message.setBody(new AmqpValue("Test " + i));
      sender.put(message);
      sender.send();
    }
    sender.stop();

    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {
    List<Message> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Message message = Message.Factory.create();
      message.setBody(new AmqpValue("Test " + i));
      message.setAddress(broker.getQueueUri("testWrite"));
      message.setSubject("test");
      data.add(message);
    }
    pipeline.apply(Create.of(data).withCoder(AmqpMessageCoder.of())).apply(AmqpIO.write());
    pipeline.run().waitUntilFinish();

    List<String> received = new ArrayList<>();
    Messenger messenger = Messenger.Factory.create();
    messenger.start();
    messenger.subscribe(broker.getQueueUri("testWrite"));
    while (received.size() < 100) {
      messenger.recv();
      while (messenger.incoming() > 0) {
        Message message = messenger.get();
        LOG.info("Received: {}", message.getBody().toString());
        received.add(message.getBody().toString());
      }
    }
    messenger.stop();

    assertEquals(100, received.size());
    for (int i = 0; i < 100; i++) {
      assertTrue(received.contains("AmqpValue{Test " + i + "}"));
    }
  }

  @Test
  public void testRestoredCheckpointAcknowledgesLiveTrackersAndEarlierCheckpoints()
      throws Exception {
    List<Tracker> accepted = new ArrayList<>();
    Messenger messenger =
        (Messenger)
            Proxy.newProxyInstance(
                Messenger.class.getClassLoader(),
                new Class<?>[] {Messenger.class},
                (proxy, method, args) -> {
                  if ("accept".equals(method.getName()) && args != null && args.length >= 1) {
                    accepted.add((Tracker) args[0]);
                  }
                  return null;
                });

    Tracker t1 =
        (Tracker)
            Proxy.newProxyInstance(
                Tracker.class.getClassLoader(), new Class<?>[] {Tracker.class}, (p, m, a) -> null);
    Tracker t2 =
        (Tracker)
            Proxy.newProxyInstance(
                Tracker.class.getClassLoader(), new Class<?>[] {Tracker.class}, (p, m, a) -> null);
    Tracker t3 =
        (Tracker)
            Proxy.newProxyInstance(
                Tracker.class.getClassLoader(), new Class<?>[] {Tracker.class}, (p, m, a) -> null);

    AmqpIO.UnboundedAmqpSource source =
        new AmqpIO.UnboundedAmqpSource(
            AmqpIO.read().withAddresses(Collections.singletonList(broker.getQueueUri("testCp"))));
    AmqpIO.AmqpCheckpointMark.Preparer preparer = new AmqpIO.AmqpCheckpointMark.Preparer();
    AtomicInteger activeCheckpoints = new AtomicInteger(0);

    preparer.add(t1);
    AmqpIO.AmqpCheckpointMark cp1 = preparer.newCheckpoint(messenger, activeCheckpoints, null);
    preparer.add(t2);
    AmqpIO.AmqpCheckpointMark cp2 = preparer.newCheckpoint(messenger, activeCheckpoints, null);
    AmqpIO.AmqpCheckpointMark emptyCp = preparer.newCheckpoint(messenger, activeCheckpoints, null);
    preparer.add(t3);
    AmqpIO.AmqpCheckpointMark cp3 = preparer.newCheckpoint(messenger, activeCheckpoints, null);

    assertEquals(3, activeCheckpoints.get());

    // Restoring cp2 and finalizing it acknowledges both cp1 and cp2, without touching cp3.
    AmqpIO.AmqpCheckpointMark restoredCp2 = CoderUtils.clone(source.getCheckpointMarkCoder(), cp2);
    restoredCp2.finalizeCheckpoint();
    assertEquals(2, accepted.size());
    assertSame(t1, accepted.get(0));
    assertSame(t2, accepted.get(1));
    assertEquals(1, activeCheckpoints.get());

    // Finalizing the empty checkpoint taken before cp3 does not duplicate acks or touch cp3.
    emptyCp.finalizeCheckpoint();
    assertEquals(2, accepted.size());
    assertEquals(1, activeCheckpoints.get());

    // Resuming from a restored cp3 in the same JVM acknowledges cp3 as well.
    AmqpIO.AmqpCheckpointMark restoredCp3 = CoderUtils.clone(source.getCheckpointMarkCoder(), cp3);
    source.createReader(PipelineOptionsFactory.create(), restoredCp3);
    assertEquals(3, accepted.size());
    assertSame(t3, accepted.get(2));
    assertEquals(0, activeCheckpoints.get());

    // Subsequent finalize on cp1 or cp3 is a no-op.
    cp1.finalizeCheckpoint();
    cp3.finalizeCheckpoint();
    assertEquals(3, accepted.size());
  }

  private static class EmbeddedAmqpBroker extends EmbeddedActiveMQBroker {
    @Override
    protected void configure() {
      try {
        getBrokerService().addConnector("amqp://localhost:0");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public String getQueueUri(String queueName) {
      return getBrokerService().getDefaultSocketURIString() + "/" + queueName;
    }
  }
}
