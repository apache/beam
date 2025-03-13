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

import java.util.Collections;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test on {@link AmqpMessageCoder}. */
@RunWith(JUnit4.class)
public class AmqpMessageCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeDecode() throws Exception {
    Message message = Message.Factory.create();
    message.setBody(new AmqpValue("body"));
    message.setAddress("address");
    message.setSubject("test");
    AmqpMessageCoder coder = AmqpMessageCoder.of();

    Message clone = CoderUtils.clone(coder, message);

    assertEquals("AmqpValue{body}", clone.getBody().toString());
    assertEquals("address", clone.getAddress());
    assertEquals("test", clone.getSubject());
  }

  @Test
  public void encodeDecodeTooMuchLargerMessage() throws Exception {
    thrown.expect(CoderException.class);
    Message message = Message.Factory.create();
    message.setAddress("address");
    message.setSubject("subject");
    String body = Joiner.on("").join(Collections.nCopies(64 * 1024 * 1024, " "));
    message.setBody(new AmqpValue(body));

    AmqpMessageCoder coder = AmqpMessageCoder.of();

    CoderUtils.encodeToByteArray(coder, message);
  }

  @Test
  public void encodeDecodeLargeMessage() throws Exception {
    Message message = Message.Factory.create();
    message.setAddress("address");
    message.setSubject("subject");
    String body = Joiner.on("").join(Collections.nCopies(32 * 1024 * 1024, " "));
    message.setBody(new AmqpValue(body));

    AmqpMessageCoder coder = AmqpMessageCoder.of();

    Message clone = CoderUtils.clone(coder, message);

    assertEquals(message.getBody().toString(), clone.getBody().toString());
  }
}
