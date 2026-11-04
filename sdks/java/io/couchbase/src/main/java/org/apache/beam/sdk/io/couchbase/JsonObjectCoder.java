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
package org.apache.beam.sdk.io.couchbase;

import com.couchbase.client.java.json.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * A {@link Coder} that encodes a {@link JsonObject} as its JSON string in UTF-8.
 *
 * <p>The encoding is 2 to 3 times smaller than {@code SerializableCoder}, which Beam would
 * otherwise infer, and does not depend on Java serialization of Couchbase SDK classes.
 *
 * <p>Decoding preserves the JSON content but not always the Java number type: numbers are decoded
 * as {@code Integer} when they fit, so a {@code Long} value of {@code 10} decodes as an {@code
 * Integer}. Documents read from Couchbase already use the decoded types.
 */
public class JsonObjectCoder extends AtomicCoder<JsonObject> {

  private static final JsonObjectCoder INSTANCE = new JsonObjectCoder();
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  public static JsonObjectCoder of() {
    return INSTANCE;
  }

  private JsonObjectCoder() {}

  @Override
  public void encode(JsonObject value, OutputStream outStream) throws IOException {
    STRING_CODER.encode(value.toString(), outStream);
  }

  @Override
  public JsonObject decode(InputStream inStream) throws IOException {
    return JsonObject.fromJson(STRING_CODER.decode(inStream));
  }

  /** Field order in the encoded JSON is not stable, so equal objects can encode differently. */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "JSON field order is not deterministic");
  }
}
