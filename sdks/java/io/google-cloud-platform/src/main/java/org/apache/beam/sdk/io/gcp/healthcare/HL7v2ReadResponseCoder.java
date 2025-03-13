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
package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Coder for {@link HL7v2ReadResponse}. */
public class HL7v2ReadResponseCoder extends CustomCoder<HL7v2ReadResponse> {

  HL7v2ReadResponseCoder() {}

  public static HL7v2ReadResponseCoder of() {
    return new HL7v2ReadResponseCoder();
  }

  public static HL7v2ReadResponseCoder of(Class<HL7v2ReadResponse> clazz) {
    return new HL7v2ReadResponseCoder();
  }

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final HL7v2MessageCoder HL7V2_MESSAGE_CODER = HL7v2MessageCoder.of();

  @Override
  public void encode(HL7v2ReadResponse value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_CODER.encode(value.getMetadata(), outStream);
    HL7V2_MESSAGE_CODER.encode(value.getHL7v2Message(), outStream);
  }

  @Override
  @SuppressWarnings("nullness") // Message is not annotated to allow nulls, but it does everywhere
  public HL7v2ReadResponse decode(InputStream inStream) throws CoderException, IOException {
    String metadata = STRING_CODER.decode(inStream);
    HL7v2Message message = HL7V2_MESSAGE_CODER.decode(inStream);
    return HL7v2ReadResponse.of(metadata, message);
  }
}
