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
package org.apache.beam.examples.complete.cdap.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Custom {@link org.apache.beam.sdk.coders.Coder} for {@link JsonElement}. */
public class JsonElementCoder extends CustomCoder<JsonElement> {
  private static final JsonElementCoder CODER = new JsonElementCoder();
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  public static JsonElementCoder of() {
    return CODER;
  }

  @Override
  public void encode(JsonElement value, @NonNull OutputStream outStream) throws IOException {
    if (value != null) {
      STRING_CODER.encode(value.toString(), outStream);
    }
  }

  @Override
  public JsonElement decode(@NonNull InputStream inStream) throws IOException {
    return JsonParser.parseString(STRING_CODER.decode(inStream));
  }
}
