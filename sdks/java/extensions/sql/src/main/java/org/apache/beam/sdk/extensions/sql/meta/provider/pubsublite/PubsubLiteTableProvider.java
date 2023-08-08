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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoOneOf;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.EquivalenceNullablePolicy;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.DeadLetteredTransform;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Pub/Sub Lite table provider.
 *
 * <p>Pub/Sub Lite tables may be constructed with:
 *
 * <pre>{@code
 * CREATE EXTERNAL TABLE tableName(
 *     message_key BYTES [NOT NULL],  // optional, always present on read
 *     publish_timestamp TIMESTAMP [NOT NULL],  // optional, readable tables only, always present on read
 *     event_timestamp TIMESTAMP [NOT NULL],  // optional, null if not present in readable table, unset in message if null in writable table. NOT NULL enforces field presence on read
 *     attributes ARRAY<ROW<key VARCHAR, values ARRAY<BYTES>>>,  // optional, null values never present on reads or handled on writes
 *     payload BYTES | ROW<[INSERT SCHEMA HERE]>,
 * )
 * TYPE pubsublite
 * // For writable tables
 * LOCATION 'projects/[PROJECT]/locations/[CLOUD ZONE]/topics/[TOPIC]'
 * // For readable tables
 * LOCATION 'projects/[PROJECT]/locations/[CLOUD ZONE]/subscriptions/[SUBSCRIPTION]'
 * TBLPROPERTIES '{
 *     "deadLetterQueue": "[DLQ_KIND]:[DLQ_ID]",  // optional
 *     "format": "[FORMAT]",  // optional
 *     // format params
 * }'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class PubsubLiteTableProvider extends InMemoryMetaTableProvider {
  @Override
  public String getTableType() {
    return "pubsublite";
  }

  private static Optional<PayloadSerializer> getSerializer(Schema schema, JSONObject properties) {
    if (schema.getField("payload").getType().equals(FieldType.BYTES)) {
      checkArgument(
          !properties.containsKey("format"),
          "Must not set the 'format' property if not unpacking payload.");
      return Optional.empty();
    }
    String format = properties.containsKey("format") ? properties.getString("format") : "json";
    return Optional.of(PayloadSerializers.getSerializer(format, schema, properties.getInnerMap()));
  }

  private static void checkFieldHasType(Field field, FieldType type) {
    checkArgument(
        type.equivalent(field.getType(), EquivalenceNullablePolicy.WEAKEN),
        String.format("'%s' field must have schema matching '%s'.", field.getName(), type));
  }

  private static void validateSchema(Schema schema) {
    checkArgument(
        schema.hasField(RowHandler.PAYLOAD_FIELD),
        "Must provide a 'payload' field for Pub/Sub Lite.");
    for (Field field : schema.getFields()) {
      switch (field.getName()) {
        case RowHandler.ATTRIBUTES_FIELD:
          checkFieldHasType(field, RowHandler.ATTRIBUTES_FIELD_TYPE);
          break;
        case RowHandler.EVENT_TIMESTAMP_FIELD:
        case RowHandler.PUBLISH_TIMESTAMP_FIELD:
          checkFieldHasType(field, FieldType.DATETIME);
          break;
        case RowHandler.MESSAGE_KEY_FIELD:
          checkFieldHasType(field, FieldType.BYTES);
          break;
        case RowHandler.PAYLOAD_FIELD:
          checkArgument(
              FieldType.BYTES.equivalent(field.getType(), EquivalenceNullablePolicy.WEAKEN)
                  || field.getType().getTypeName().equals(TypeName.ROW),
              String.format(
                  "'%s' field must either have a 'BYTES NOT NULL' or 'ROW' schema.",
                  field.getName()));
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "'%s' field is invalid at the top level for Pub/Sub Lite.", field.getName()));
      }
    }
  }

  @AutoOneOf(Location.Kind.class)
  abstract static class Location {
    enum Kind {
      TOPIC,
      SUBSCRIPTION
    }

    abstract Kind getKind();

    abstract TopicPath topic();

    abstract SubscriptionPath subscription();

    static Location parse(String location) {
      if (location.contains("/topics/")) {
        return AutoOneOf_PubsubLiteTableProvider_Location.topic(TopicPath.parse(location));
      }
      if (location.contains("/subscriptions/")) {
        return AutoOneOf_PubsubLiteTableProvider_Location.subscription(
            SubscriptionPath.parse(location));
      }
      throw new IllegalArgumentException(
          String.format(
              "Location '%s' does not correspond to either a Pub/Sub Lite topic or subscription.",
              location));
    }
  }

  private static RowHandler getRowHandler(
      Schema schema, Optional<PayloadSerializer> optionalSerializer) {
    if (optionalSerializer.isPresent()) {
      return new RowHandler(schema, optionalSerializer.get());
    }
    return new RowHandler(schema);
  }

  private static <InputT, OutputT>
      PTransform<PCollection<? extends InputT>, PCollection<OutputT>> addDlqIfPresent(
          SimpleFunction<InputT, OutputT> transform, JSONObject properties) {
    if (properties.containsKey("deadLetterQueue")) {
      return new DeadLetteredTransform<>(transform, properties.getString("deadLetterQueue"));
    }
    return MapElements.via(transform);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    checkArgument(table.getType().equals(getTableType()));
    validateSchema(table.getSchema());
    Optional<PayloadSerializer> serializer =
        getSerializer(table.getSchema(), table.getProperties());
    Location location = Location.parse(checkArgumentNotNull(table.getLocation()));
    RowHandler rowHandler = getRowHandler(table.getSchema(), serializer);

    switch (location.getKind()) {
      case TOPIC:
        checkArgument(
            !table.getSchema().hasField(RowHandler.PUBLISH_TIMESTAMP_FIELD),
            "May not write to publish timestamp, this field is read-only.");
        return new PubsubLiteTopicTable(
            table.getSchema(),
            location.topic(),
            addDlqIfPresent(
                SimpleFunction.fromSerializableFunctionWithOutputType(
                    rowHandler::rowToMessage, TypeDescriptor.of(PubSubMessage.class)),
                table.getProperties()));
      case SUBSCRIPTION:
        return new PubsubLiteSubscriptionTable(
            table.getSchema(),
            location.subscription(),
            addDlqIfPresent(
                SimpleFunction.fromSerializableFunctionWithOutputType(
                    rowHandler::messageToRow, TypeDescriptor.of(Row.class)),
                table.getProperties()));
      default:
        throw new IllegalArgumentException("Invalid kind for location: " + location.getKind());
    }
  }
}
