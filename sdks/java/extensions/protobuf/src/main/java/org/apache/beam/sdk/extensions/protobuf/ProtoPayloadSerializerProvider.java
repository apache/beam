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
package org.apache.beam.sdk.extensions.protobuf;

import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@Internal
@AutoService(PayloadSerializerProvider.class)
public class ProtoPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "proto";
  }

  private static Class<? extends Message> getClass(Map<String, Object> tableParams) {
    String protoClassName = checkArgumentNotNull(tableParams.get("protoClass")).toString();
    try {
      Class<?> protoClass = Class.forName(protoClassName);
      return protoClass.asSubclass(Message.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Incorrect proto class provided: " + protoClassName, e);
    }
  }

  private static <T extends Message> void inferAndVerifySchema(
      Class<T> protoClass, Schema requiredSchema) {
    @Nonnull
    Schema inferredSchema =
        checkArgumentNotNull(new ProtoMessageSchema().schemaFor(TypeDescriptor.of(protoClass)));
    if (!inferredSchema.assignableTo(requiredSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "Given message schema: '%s'%n"
                  + "does not match schema inferred from protobuf class.%n"
                  + "Protobuf class: '%s'%n"
                  + "Inferred schema: '%s'",
              requiredSchema, protoClass.getName(), inferredSchema));
    }
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> tableParams) {
    Class<? extends Message> protoClass = getClass(tableParams);
    inferAndVerifySchema(protoClass, schema);
    SimpleFunction<byte[], Row> toRowFn = ProtoMessageSchema.getProtoBytesToRowFn(protoClass);
    return PayloadSerializer.of(
        ProtoMessageSchema.getRowToProtoBytesFn(protoClass),
        bytes -> {
          Row rawRow = toRowFn.apply(bytes);
          return castRow(rawRow, rawRow.getSchema(), schema);
        });
  }
}
