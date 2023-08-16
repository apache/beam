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
package org.apache.beam.sdk.io.aws.sqs;

import static com.amazonaws.services.sqs.model.MessageSystemAttributeName.SentTimestamp;
import static org.apache.beam.sdk.io.aws.sqs.SqsUnboundedReader.REQUEST_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.util.Random;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class SqsMessageCoderTest {

  @Test
  public void testMessageDecodeEncodeEquals() throws Exception {
    Message message =
        new Message()
            .withMessageId("messageId")
            .withReceiptHandle("receiptHandle")
            .withBody("body")
            .withAttributes(
                ImmutableMap.of(SentTimestamp.name(), Long.toString(new Random().nextLong())))
            .withMessageAttributes(
                ImmutableMap.of(
                    REQUEST_TIME,
                    new MessageAttributeValue()
                        .withStringValue(Long.toString(new Random().nextLong()))));

    Message clone = CoderUtils.clone(SqsMessageCoder.of(), message);
    assertThat(clone).isEqualTo(message);
  }

  @Test
  public void testVerifyDeterministic() throws Exception {
    SqsMessageCoder.of().verifyDeterministic(); // must not throw
  }

  @Test
  public void testConsistentWithEquals() {
    // some attributes might be omitted
    assertThat(SqsMessageCoder.of().consistentWithEquals()).isFalse();
  }
}
