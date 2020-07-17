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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.healthcare.v1beta1.model.ParsedData;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class ParsedDataCoder extends CustomCoder<ParsedData> {

  public static org.apache.beam.sdk.io.gcp.healthcare.ParsedDataCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(ParsedData value, OutputStream outStream) throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(ParsedData value, OutputStream outStream, Context context) throws IOException {
    String strValue = MAPPER.writeValueAsString(value);
    StringUtf8Coder.of().encode(strValue, outStream, context);
  }

  @Override
  public ParsedData decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public ParsedData decode(InputStream inStream, Context context) throws IOException {
    String strValue = StringUtf8Coder.of().decode(inStream, context);
    return MAPPER.readValue(strValue, ParsedData.class);
  }

  @Override
  public long getEncodedElementByteSize(ParsedData value) throws Exception {
    String strValue = MAPPER.writeValueAsString(value);
    return StringUtf8Coder.of().getEncodedElementByteSize(strValue);
  }

  /////////////////////////////////////////////////////////////////////////////

  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
  // ParsedData.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private static final org.apache.beam.sdk.io.gcp.healthcare.ParsedDataCoder INSTANCE =
      new org.apache.beam.sdk.io.gcp.healthcare.ParsedDataCoder();
  private static final TypeDescriptor<ParsedData> TYPE_DESCRIPTOR =
      new TypeDescriptor<ParsedData>() {};

  private ParsedDataCoder() {}

  @Override
  public TypeDescriptor<ParsedData> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
