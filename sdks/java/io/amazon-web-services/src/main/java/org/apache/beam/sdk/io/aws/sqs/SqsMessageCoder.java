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

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Deterministic coder for an AWS Sdk SQS message.
 *
 * <p>This encoder only keeps the `SentTimestamp` attribute as well as the `requestTimeMsSinceEpoch`
 * message attribute, other attributes are dropped. You may provide your own coder in case you need
 * to access further attributes.
 */
class SqsMessageCoder extends AtomicCoder<Message> {
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> OPT_STRING_CODER =
      NullableCoder.of(StringUtf8Coder.of());

  private static final Coder<Message> INSTANCE = new SqsMessageCoder();

  static Coder<Message> of() {
    return INSTANCE;
  }

  private SqsMessageCoder() {}

  @Override
  public void encode(Message value, OutputStream out) throws IOException {
    STRING_CODER.encode(value.getMessageId(), out);
    STRING_CODER.encode(value.getReceiptHandle(), out);
    OPT_STRING_CODER.encode(value.getBody(), out);
    OPT_STRING_CODER.encode(value.getAttributes().get(SentTimestamp.toString()), out);
    MessageAttributeValue reqTime = value.getMessageAttributes().get(REQUEST_TIME);
    OPT_STRING_CODER.encode(reqTime != null ? reqTime.getStringValue() : null, out);
  }

  @Override
  public Message decode(InputStream in) throws IOException {
    Message msg = new Message();
    msg.setMessageId(STRING_CODER.decode(in));
    msg.setReceiptHandle(STRING_CODER.decode(in));

    // SQS library not annotated, but this coder assumes null is allowed (documentation does not
    // specify)
    @SuppressWarnings("nullness")
    @NonNull
    String body = OPT_STRING_CODER.decode(in);
    msg.setBody(body);

    String sentAt = OPT_STRING_CODER.decode(in);
    if (sentAt != null) {
      msg.addAttributesEntry(SentTimestamp.toString(), sentAt);
    }

    String reqTime = OPT_STRING_CODER.decode(in);
    if (reqTime != null) {
      msg.addMessageAttributesEntry(
          REQUEST_TIME, new MessageAttributeValue().withStringValue(reqTime));
    }
    return msg;
  }
}
