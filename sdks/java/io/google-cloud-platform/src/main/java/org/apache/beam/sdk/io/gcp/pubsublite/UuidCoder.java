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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A coder for a Uuid. */
public class UuidCoder extends AtomicCoder<Uuid> {
  private static Coder<Uuid> coder = DelegateCoder.of(ByteStringCoder.of(), Uuid::value, Uuid::of);

  @Override
  public void encode(Uuid value, OutputStream outStream) throws IOException {
    coder.encode(value, outStream);
  }

  @Override
  public Uuid decode(InputStream inStream) throws IOException {
    return coder.decode(inStream);
  }

  public static CoderProvider getCoderProvider() {
    return CoderProviders.forCoder(TypeDescriptor.of(Uuid.class), new UuidCoder());
  }
}
