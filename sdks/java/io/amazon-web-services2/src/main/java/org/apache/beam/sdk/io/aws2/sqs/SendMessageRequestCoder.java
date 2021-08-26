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
package org.apache.beam.sdk.io.aws2.sqs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** Custom Coder for handling SendMessageRequest for using in Write. */
public class SendMessageRequestCoder extends AtomicCoder<SendMessageRequest>
    implements Serializable {
  private static final SendMessageRequestCoder INSTANCE = new SendMessageRequestCoder();

  private SendMessageRequestCoder() {}

  static SendMessageRequestCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(SendMessageRequest value, OutputStream outStream) throws IOException {
    StringUtf8Coder.of().encode(value.queueUrl(), outStream);
    StringUtf8Coder.of().encode(value.messageBody(), outStream);
  }

  @Override
  public SendMessageRequest decode(InputStream inStream) throws IOException {
    final String queueUrl = StringUtf8Coder.of().decode(inStream);
    final String message = StringUtf8Coder.of().decode(inStream);
    return SendMessageRequest.builder().queueUrl(queueUrl).messageBody(message).build();
  }
}
