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
package org.apache.beam.sdk.io.thrift;

import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.RowMessages;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;

@Internal
@SuppressWarnings("rawtypes")
@AutoService(PayloadSerializerProvider.class)
public class ThriftPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "thrift";
  }

  private static Class<? extends TBase> getMessageClass(Map<String, Object> tableParams) {
    String thriftClassName = checkArgumentNotNull(tableParams.get("thriftClass")).toString();
    try {
      Class<?> thriftClass = Class.forName(thriftClassName);
      return thriftClass.asSubclass(TBase.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Incorrect thrift class provided: " + thriftClassName, e);
    }
  }

  private static TProtocolFactory getProtocolFactory(Map<String, Object> tableParams) {
    String thriftFactoryClassName =
        checkArgumentNotNull(tableParams.get("thriftProtocolFactoryClass")).toString();
    try {
      Class<?> thriftClass = Class.forName(thriftFactoryClassName);
      return thriftClass.asSubclass(TProtocolFactory.class).getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          "Incorrect thrift protocol factory class provided: " + thriftFactoryClassName, e);
    }
  }

  private static void inferAndVerifySchema(Class<?> thriftClass, Schema requiredSchema) {
    TypeDescriptor<?> typeDescriptor = TypeDescriptor.of(thriftClass);
    Schema schema = checkArgumentNotNull(ThriftSchema.provider().schemaFor(typeDescriptor));
    if (!schema.assignableTo(requiredSchema)) {
      throw new IllegalArgumentException(
          String.format(
              "Given message schema: '%s'%n"
                  + "does not match schema inferred from thrift class.%n"
                  + "Thrift class: '%s'%n"
                  + "Inferred schema: '%s'",
              requiredSchema, thriftClass.getName(), schema));
    }
  }

  /** A helper needed to fix the type `T` of thriftClass to satisfy RowMessages constraints. */
  private static <T extends TBase> PayloadSerializer getPayloadSerializer(
      Schema schema, TProtocolFactory protocolFactory, Class<T> thriftClass) {
    Coder<T> coder = ThriftCoder.of(thriftClass, protocolFactory);
    TypeDescriptor<T> descriptor = TypeDescriptor.of(thriftClass);
    SimpleFunction<byte[], Row> toRowFn =
        RowMessages.bytesToRowFn(ThriftSchema.provider(), descriptor, coder);
    return PayloadSerializer.of(
        RowMessages.rowToBytesFn(ThriftSchema.provider(), descriptor, coder),
        bytes -> {
          Row rawRow = toRowFn.apply(bytes);
          return castRow(rawRow, rawRow.getSchema(), schema);
        });
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> tableParams) {
    Class<? extends TBase> thriftClass = getMessageClass(tableParams);
    TProtocolFactory protocolFactory = getProtocolFactory(tableParams);
    inferAndVerifySchema(thriftClass, schema);
    return getPayloadSerializer(schema, protocolFactory, thriftClass);
  }
}
