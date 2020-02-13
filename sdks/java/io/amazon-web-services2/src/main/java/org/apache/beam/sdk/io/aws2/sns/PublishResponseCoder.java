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
package org.apache.beam.sdk.io.aws2.sns;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Custom Coder for handling publish result. */
public class PublishResponseCoder extends AtomicCoder<PublishResponse> implements Serializable {
  private static final PublishResponseCoder INSTANCE = new PublishResponseCoder();

  private PublishResponseCoder() {}

  static PublishResponseCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(PublishResponse value, OutputStream outStream) throws IOException {
    StringUtf8Coder.of().encode(value.messageId(), outStream);
  }

  @Override
  public PublishResponse decode(InputStream inStream) throws IOException {
    final String messageId = StringUtf8Coder.of().decode(inStream);
    return PublishResponse.builder().messageId(messageId).build();
  }
}
