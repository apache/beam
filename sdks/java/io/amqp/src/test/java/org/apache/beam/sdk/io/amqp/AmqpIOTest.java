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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
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
        LOG.info("Received: " + message.getBody().toString());
        received.add(message.getBody().toString());
      }
    }
    messenger.stop();

    assertEquals(100, received.size());
    for (int i = 0; i < 100; i++) {
      assertTrue(received.contains("AmqpValue{Test " + i + "}"));
    }
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
