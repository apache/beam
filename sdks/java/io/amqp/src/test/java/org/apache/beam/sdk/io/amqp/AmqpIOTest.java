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

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests on {@link AmqpIO}.
 */
@RunWith(JUnit4.class)
public class AmqpIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpIOTest.class);

  private int port;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void findFreeNetworkPort() throws Exception {
    LOG.info("Finding free network port");
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    LOG.info("Using {} port number");
    socket.close();
  }

  @Test
  public void testRead() throws Exception {
    LOG.info("Creating pipeline reading from AMQP");
    PCollection<Message> output = pipeline.apply(AmqpIO.read()
        .withMaxNumRecords(100)
        .withAddresses(Collections.singletonList("amqp://~localhost:" + port)));
    PAssert.thatSingleton(output.apply(Count.<Message>globally())).isEqualTo(100L);

    Thread sender = new Thread() {
      public void run() {
        try {
          LOG.info("Starting sender thread");
          Thread.sleep(500);
          LOG.info("Creating the AMQP messenger");
          Messenger sender = Messenger.Factory.create();
          sender.setTimeout(5000);
          LOG.info("Starting the AMQP messenger");
          sender.start();
          LOG.info("Sending messages to amqp://localhost:{}", port);
          for (int i = 0; i < 100; i++) {
            Message message = Message.Factory.create();
            message.setAddress("amqp://localhost:" + port);
            message.setBody(new AmqpValue("Test " + i));
            sender.put(message);
            sender.send();
          }
          LOG.info("Stopping the AMQP messenger");
          sender.stop();
        } catch (Exception e) {
          LOG.error("Sender error", e);
        }
      }
    };
    try {
      LOG.info("Starting sender");
      sender.start();
      LOG.info("Running pipeline");
      pipeline.run();
    } finally {
      LOG.info("Join on the sender thread");
      sender.join(5000);
    }
  }

  @Test
  public void testWrite() throws Exception {
    final List<String> received = new ArrayList<>();
    Thread receiver = new Thread() {
      @Override
      public void run() {
        try {
          LOG.info("Starting receiver thread");
          Messenger messenger = Messenger.Factory.create();
          LOG.info("Starting the AMQP messenger");
          messenger.start();
          LOG.info("Subscribing on amqp://~localhost:{}", port);
          messenger.subscribe("amqp://~localhost:" + port);
          messenger.setTimeout(5000);
          while (received.size() < 100) {
            messenger.recv();
            while (messenger.incoming() > 0) {
              Message message = messenger.get();
              received.add(message.getBody().toString());
            }
          }
          LOG.info("Stopping the AMQP messenger");
          messenger.stop();
        } catch (Exception e) {
          LOG.error("Receiver error", e);
        }
      }
    };
    LOG.info("Starting receiver");
    receiver.start();

    LOG.info("Creating message to send");
    List<Message> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Message message = Message.Factory.create();
      message.setBody(new AmqpValue("Test " + i));
      message.setAddress("amqp://localhost:" + port);
      message.setSubject("test");
      data.add(message);
    }
    LOG.info("Creating pipeline writing to AMQP");
    pipeline.apply(Create.of(data).withCoder(AmqpMessageCoder.of())).apply(AmqpIO.write());
    LOG.info("Starting pipeline");
    try {
      LOG.info("Running pipeline");
      pipeline.run();
    } finally {
      LOG.info("Join receiver thread");
      receiver.join(5000);
    }

    assertEquals(100, received.size());
    for (int i = 0; i < 100; i++) {
      assertTrue(received.contains("AmqpValue{Test " + i + "}"));
    }
  }

}
