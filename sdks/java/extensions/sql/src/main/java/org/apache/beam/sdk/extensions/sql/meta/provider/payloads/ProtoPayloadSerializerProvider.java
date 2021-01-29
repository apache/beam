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
package org.apache.beam.sdk.extensions.sql.meta.provider.payloads;

import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.protobuf.ProtoDomain;
import org.apache.beam.sdk.extensions.protobuf.ProtoDynamicMessageSchema;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@Internal
@Experimental(Kind.SCHEMAS)
@AutoService(PayloadSerializerProvider.class)
public class ProtoPayloadSerializerProvider implements PayloadSerializerProvider {
  @Override
  public String identifier() {
    return "proto";
  }

  private static Class<? extends Message> getClassFromName(String protoClassName) {
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

  private static PayloadSerializer getProtoClassSerializer(Schema schema, String protoClass) {
    Class<? extends Message> klass = getClassFromName(protoClass);
    inferAndVerifySchema(klass, schema);
    SimpleFunction<byte[], Row> toRowFn = ProtoMessageSchema.getProtoBytesToRowFn(klass);
    return PayloadSerializer.of(
        ProtoMessageSchema.getRowToProtoBytesFn(klass),
        bytes -> castRow(toRowFn.apply(bytes), schema));
  }

  private static PayloadSerializer getDescriptorSetSerializer(
      Schema schema, String protoDescriptorSetFile, String protoMessageName) {
    try {
      MatchResult result = FileSystems.match(protoDescriptorSetFile, EmptyMatchTreatment.DISALLOW);
      if (result.metadata().size() != 1) {
        throw new IllegalArgumentException(
            String.format(
                "protoDescriptorSetFile must point to a single file containing the binary proto message. `%s` matched %s files.",
                protoDescriptorSetFile, result.metadata().size()));
      }
      Metadata metadata = result.metadata().iterator().next();
      FileDescriptorSet descriptorSet;
      try (InputStream is = Channels.newInputStream(FileSystems.open(metadata.resourceId()))) {
        descriptorSet = FileDescriptorSet.parseFrom(is);
      }
      ProtoDomain domain = ProtoDomain.buildFrom(descriptorSet);
      Optional<Descriptor> descriptor = Optional.ofNullable(domain.getDescriptor(protoMessageName));
      if (!descriptor.isPresent()) {
        throw new IllegalArgumentException(
            String.format(
                "Message '%s' does not exist in descriptor set loaded from '%s'.",
                protoMessageName, protoDescriptorSetFile));
      }
      ProtoDynamicMessageSchema messageSchema =
          ProtoDynamicMessageSchema.forDescriptor(domain, descriptor.get());
      if (!messageSchema.getSchema().assignableTo(schema)) {
        throw new IllegalArgumentException(
            String.format(
                "Given message schema: '%s'%n"
                    + "does not match schema inferred from protobuf message.%n"
                    + "Protobuf message: '%s'%n"
                    + "Inferred schema: '%s'",
                schema, protoMessageName, messageSchema.getSchema()));
      }
      return PayloadSerializer.of(
          messageSchema.getRowToBytesFunction(),
          bytes -> castRow(messageSchema.getBytesToRowFunction().apply(bytes), schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to load descriptor set.", e);
    }
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> tableParams) {
    Optional<Object> protoClassName = Optional.ofNullable(tableParams.get("protoClass"));
    Optional<Object> protoDescriptorSetFile =
        Optional.ofNullable(tableParams.get("protoDescriptorSetFile"));
    Optional<Object> protoMessageName = Optional.ofNullable(tableParams.get("protoMessageName"));
    String argumentError =
        "Must provide either protoClass corresponding to a class built into the jar or both "
            + "protoDescriptorSetFile and protoMessageName with a path to the descriptor set and "
            + "namespaced proto message name respectively.";
    if (protoClassName.isPresent()) {
      checkArgument(!protoDescriptorSetFile.isPresent(), argumentError);
      checkArgument(!protoMessageName.isPresent(), argumentError);
      return getProtoClassSerializer(schema, protoClassName.get().toString());
    }
    checkArgument(protoDescriptorSetFile.isPresent(), argumentError);
    checkArgument(protoMessageName.isPresent(), argumentError);
    return getDescriptorSetSerializer(
        schema, protoDescriptorSetFile.get().toString(), protoMessageName.get().toString());
  }
}
