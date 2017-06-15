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

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test on the {@link AmqpMessageCoder}.
 */
public class AmqpMessageCoderTest {

  private final AmqpMessageCoder coder = AmqpMessageCoder.of();

  @Test
  public void testEncodeDecode() throws Exception {
    Message message = Message.Factory.create();
    message.setBody(new AmqpValue("test"));
    FileOutputStream fileOutputStream = new FileOutputStream("target/coder-test");
    coder.encode(message, fileOutputStream);
    fileOutputStream.close();

    FileInputStream fileInputStream = new FileInputStream("target/coder-test");
    message = coder.decode(fileInputStream);

    assertEquals(message.getBody().toString(), "AmqpValue{test}");
  }

  @Test
  @Ignore("Fails with EOFException on VarInt (line 82)")
  public void testEncodeDecodeWithLargeMessage() throws Exception {
    Message message = Message.Factory.create();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      builder.append("test").append("\n");
    }
    message.setBody(new AmqpValue(builder.toString()));
    FileOutputStream fileOutputStream = new FileOutputStream("target/coder-large-test");
    coder.encode(message, fileOutputStream);
    fileOutputStream.close();

    FileInputStream fileInputStream = new FileInputStream("target/coder-large-test");
    message = coder.decode(fileInputStream);

    System.out.println(message.getBody().toString());
  }

}
