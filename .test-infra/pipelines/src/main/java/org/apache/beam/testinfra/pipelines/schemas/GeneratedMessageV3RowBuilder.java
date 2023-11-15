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
package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.KEY_FIELD_NAME;
import static org.apache.beam.testinfra.pipelines.schemas.DescriptorSchemaRegistry.VALUE_FIELD_NAME;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

/** Converts a {@link GeneratedMessageV3} type to a {@link Row}. */
@Internal
public class GeneratedMessageV3RowBuilder<T extends GeneratedMessageV3> {
  public static <T extends GeneratedMessageV3> GeneratedMessageV3RowBuilder<T> of(T source) {
    return new GeneratedMessageV3RowBuilder<>(source);
  }

  private static final Map<JavaType, Object> DEFAULT_VALUES =
      ImmutableMap.<JavaType, Object>builder()
          .put(JavaType.BOOLEAN, false)
          .put(JavaType.INT, 0)
          .put(JavaType.LONG, 0L)
          .put(JavaType.FLOAT, 0f)
          .put(JavaType.DOUBLE, 0.0)
          .put(JavaType.ENUM, "")
          .put(JavaType.STRING, "")
          .build();

  private final ExecutorService threadPool = Executors.newSingleThreadExecutor();
  private final ListeningExecutorService service = MoreExecutors.listeningDecorator(threadPool);

  private static final Map<@NonNull String, Function<Object, Object>> CONVERTERS_MAP =
      ImmutableMap.<String, Function<Object, Object>>builder()
          .put("google.protobuf.Value", o -> convert((Value) o))
          .put("google.protobuf.Any", o -> convert((Any) o))
          .put("google.protobuf.Timestamp", o -> convert((Timestamp) o))
          .build();
  private final @NonNull T source;

  private Row.@NonNull FieldValueBuilder builder;

  GeneratedMessageV3RowBuilder(@NonNull T source) {
    this.source = source;
    Schema schema =
        checkStateNotNull(
            DescriptorSchemaRegistry.INSTANCE.getOrBuild(source.getDescriptorForType()));
    builder = Row.withSchema(schema).withFieldValues(ImmutableMap.of());
  }

  /**
   * Builds a {@link Row} from a {@link GeneratedMessageV3} type submitting nested types to an
   * {@link ExecutorService} to prevent stack overflow errors.
   */
  public Row build() {
    for (FieldDescriptor fieldDescriptor : source.getDescriptorForType().getFields()) {
      if (shouldSkip(fieldDescriptor)) {
        continue;
      }
      Object value = getValue(fieldDescriptor);
      builder.withFieldValue(fieldDescriptor.getName(), value);
    }

    Row result = builder.build();
    shutdownAwaitTermination();
    return result;
  }

  Object getValue(FieldDescriptor fieldDescriptor) {
    return getValue(this.source, fieldDescriptor);
  }

  <MessageT extends AbstractMessage> Object getValue(
      MessageT message, FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.isMapField()) {
      return mapOf(message, fieldDescriptor);
    }
    if (fieldDescriptor.isRepeated()) {
      return listOf(message, fieldDescriptor);
    }
    Object value = message.getField(fieldDescriptor);
    return convert(fieldDescriptor, value);
  }

  <MessageT extends AbstractMessage> Object listOf(
      MessageT message, FieldDescriptor fieldDescriptor) {
    List<Object> result = new ArrayList<>();
    int size = message.getRepeatedFieldCount(fieldDescriptor);
    for (int i = 0; i < size; i++) {
      Object value = getValue(message, fieldDescriptor, i);
      result.add(value);
    }
    return result;
  }

  <MessageT extends AbstractMessage> Object getValue(
      MessageT message, FieldDescriptor fieldDescriptor, int i) {
    Object value = message.getRepeatedField(fieldDescriptor, i);
    return convert(fieldDescriptor, value);
  }

  <MessageT extends AbstractMessage> Object mapOf(
      MessageT message, FieldDescriptor fieldDescriptor) {
    List<Row> result = new ArrayList<>();
    Descriptor mapDescriptor = fieldDescriptor.getMessageType();
    FieldDescriptor keyType = checkStateNotNull(mapDescriptor.findFieldByName(KEY_FIELD_NAME));
    FieldDescriptor valueType = checkStateNotNull(mapDescriptor.findFieldByName(VALUE_FIELD_NAME));
    int size = message.getRepeatedFieldCount(fieldDescriptor);
    Schema messageSchema =
        checkStateNotNull(
            DescriptorSchemaRegistry.INSTANCE.getOrBuild(message.getDescriptorForType()));
    Schema.Field mapField = messageSchema.getField(fieldDescriptor.getName());
    checkState(mapField.getType().getTypeName().equals(TypeName.ARRAY));
    Schema.FieldType mapEntryFieldType =
        checkStateNotNull(mapField.getType().getCollectionElementType());
    checkState(mapEntryFieldType.getTypeName().equals(TypeName.ROW));
    Schema entrySchema = checkStateNotNull(mapEntryFieldType.getRowSchema());

    for (int i = 0; i < size; i++) {
      Object entryObj = message.getRepeatedField(fieldDescriptor, i);
      checkState(
          entryObj instanceof AbstractMessage,
          "%s is not an instance of %s, found: %s",
          fieldDescriptor.getName(),
          AbstractMessage.class,
          entryObj.getClass());
      AbstractMessage entry = (AbstractMessage) entryObj;
      Object key = getValue(entry, keyType);
      Object value = getValue(entry, valueType);
      Row entryRow =
          Row.withSchema(entrySchema)
              .withFieldValue(KEY_FIELD_NAME, key)
              .withFieldValue(VALUE_FIELD_NAME, value)
              .build();
      result.add(entryRow);
    }
    return result;
  }

  Object convert(FieldDescriptor fieldDescriptor, Object originalValue) {

    if (originalValue == null && DEFAULT_VALUES.containsKey(fieldDescriptor.getJavaType())) {
      return checkStateNotNull(DEFAULT_VALUES.get(fieldDescriptor.getJavaType()));
    }

    if (fieldDescriptor.getJavaType().equals(JavaType.ENUM)) {
      return checkStateNotNull(originalValue).toString();
    }

    if (!fieldDescriptor.getJavaType().equals(JavaType.MESSAGE)) {
      return originalValue;
    }

    Descriptor descriptor = fieldDescriptor.getMessageType();
    if (CONVERTERS_MAP.containsKey(descriptor.getFullName())) {
      Function<Object, Object> converter =
          checkStateNotNull(CONVERTERS_MAP.get(descriptor.getFullName()));
      return converter.apply(originalValue);
    }

    checkState(
        originalValue instanceof GeneratedMessageV3,
        "%s is not instance of %s, found: %s",
        fieldDescriptor.getName(),
        GeneratedMessageV3.class,
        originalValue.getClass());

    GeneratedMessageV3 message = (GeneratedMessageV3) originalValue;
    GeneratedMessageV3RowBuilder<GeneratedMessageV3> rowBuilder =
        new GeneratedMessageV3RowBuilder<>(message);

    try {
      return service.submit(rowBuilder::build).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(
          String.format(
              "error building Row of %s type for field: %s of %s: %s %s",
              fieldDescriptor.getMessageType().getFullName(),
              fieldDescriptor.getName(),
              source.getDescriptorForType().getFullName(),
              e.getMessage(),
              Throwables.getStackTraceAsString(e)));
    }
  }

  boolean shouldSkip(FieldDescriptor fieldDescriptor) {
    return fieldDescriptor.getJavaType().equals(JavaType.BYTE_STRING);
  }

  private static Object convert(Timestamp timestamp) {
    return Instant.ofEpochMilli(timestamp.getSeconds() * 1000);
  }

  private static Object convert(Message value) {
    try {
      return JsonFormat.printer().omittingInsignificantWhitespace().print(value);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  private void shutdownAwaitTermination() {
    if (shutdownConsumeTerminationInterruptIfNeeded(
        service,
        e -> {
          shutdownAndIgnoreInterruptIfNeeded(threadPool);
        })) {
      shutdownAndIgnoreInterruptIfNeeded(threadPool);
    }
  }

  private boolean shutdownConsumeTerminationInterruptIfNeeded(
      ExecutorService executorService, Consumer<InterruptedException> handleInterrupt) {
    try {
      if (executorService.isShutdown()) {
        return true;
      }
      executorService.shutdown();
      return executorService.awaitTermination(1L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      handleInterrupt.accept(e);
      return true;
    }
  }

  private void shutdownAndIgnoreInterruptIfNeeded(ExecutorService executorService) {
    try {
      if (executorService.isShutdown()) {
        return;
      }
      executorService.shutdown();
      boolean ignored = executorService.awaitTermination(1L, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
  }
}
