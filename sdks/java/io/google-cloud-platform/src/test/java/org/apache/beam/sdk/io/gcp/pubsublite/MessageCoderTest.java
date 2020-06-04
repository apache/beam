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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.cloud.pubsublite.Message;
import com.google.common.collect.ImmutableListMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MessageCoderTest {
  @Test
  public void roundTripMessages() throws IOException {
    Message message1 =
        Message.builder()
            .setKey(ByteString.copyFromUtf8("abc"))
            .setData(ByteString.copyFromUtf8("def"))
            .setEventTime(Timestamps.EPOCH)
            .setAttributes(
                ImmutableListMultimap.<String, ByteString>builder()
                    .put("xy", ByteString.copyFromUtf8("z"))
                    .build())
            .build();
    Message message2 = message1.toBuilder().setKey(ByteString.copyFromUtf8("eeeeee")).build();
    MessageCoder coder = new MessageCoder();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(message1, output);
    coder.encode(message2, output);
    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    assertThat(message1, equalTo(coder.decode(input)));
    assertThat(message2, equalTo(coder.decode(input)));
  }
}
