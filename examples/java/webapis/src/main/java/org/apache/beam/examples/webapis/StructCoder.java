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
package org.apache.beam.examples.webapis;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

class StructCoder extends CustomCoder<Struct> {

  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  static StructCoder of(String field, String... fields) {
    List<String> fieldList = new ArrayList<>();
    fieldList.add(field);
    fieldList.addAll(Arrays.asList(fields));
    return new StructCoder(fieldList);
  }

  private final List<String> fields;

  private StructCoder(List<String> fields) {
    this.fields = fields;
  }

  @Override
  public void encode(Struct value, OutputStream outStream) throws CoderException, IOException {
    Map<String, Value> fieldValues = value.getFieldsMap();
    for (String field : fields) {
      Value fieldValue = checkStateNotNull(fieldValues.get(field));
      STRING_CODER.encode(fieldValue.getStringValue(), outStream);
    }
  }

  @Override
  public Struct decode(InputStream inStream) throws CoderException, IOException {
    Struct.Builder builder = Struct.newBuilder();
    for (String field : fields) {
      Value value = Value.newBuilder().setStringValue(STRING_CODER.decode(inStream)).build();
      builder.putFields(field, value);
    }
    return builder.build();
  }
}
