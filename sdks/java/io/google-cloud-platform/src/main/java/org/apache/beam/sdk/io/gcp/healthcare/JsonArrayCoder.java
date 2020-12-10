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

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class JsonArrayCoder extends CustomCoder<JsonArray> {
  private static final JsonArrayCoder CODER = new JsonArrayCoder();
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  public static JsonArrayCoder of() {
    return CODER;
  }

  @Override
  public void encode(JsonArray value, OutputStream outStream) throws IOException {
    STRING_CODER.encode(value.toString(), outStream);
  }

  @Override
  public JsonArray decode(InputStream inStream) throws IOException {
    return JsonParser.parseString(STRING_CODER.decode(inStream)).getAsJsonArray();
  }
}
