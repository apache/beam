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
package org.apache.beam.sdk.io.aws.sns;

import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Custom Coder for handling publish result. */
public class PublishResultCoder extends Coder<PublishResult> implements Serializable {
  private static final PublishResultCoder INSTANCE = new PublishResultCoder();

  private PublishResultCoder() {}

  static PublishResultCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(PublishResult value, OutputStream outStream)
      throws CoderException, IOException {
    StringUtf8Coder.of().encode(value.getMessageId(), outStream);
  }

  @Override
  public PublishResult decode(InputStream inStream) throws CoderException, IOException {
    final String messageId = StringUtf8Coder.of().decode(inStream);
    return new PublishResult().withMessageId(messageId);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    StringUtf8Coder.of().verifyDeterministic();
  }
}
