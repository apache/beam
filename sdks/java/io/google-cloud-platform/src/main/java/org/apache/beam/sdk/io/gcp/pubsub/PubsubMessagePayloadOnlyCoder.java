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
package org.apache.beam.sdk.io.gcp.pubsub;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.StreamUtils;

/** A coder for PubsubMessage treating the raw bytes being decoded as the message's payload. */
public class PubsubMessagePayloadOnlyCoder extends CustomCoder<PubsubMessage> {
  public static PubsubMessagePayloadOnlyCoder of() {
    return new PubsubMessagePayloadOnlyCoder();
  }

  @Override
  public void encode(PubsubMessage value, OutputStream outStream, Context context)
      throws IOException {
    checkState(context.isWholeStream, "Expected to only be used in a whole-stream context");
    outStream.write(value.getPayload());
  }

  @Override
  public PubsubMessage decode(InputStream inStream, Context context) throws IOException {
    checkState(context.isWholeStream, "Expected to only be used in a whole-stream context");
    return new PubsubMessage(
        StreamUtils.getBytes(inStream), ImmutableMap.<String, String>of());
  }
}
