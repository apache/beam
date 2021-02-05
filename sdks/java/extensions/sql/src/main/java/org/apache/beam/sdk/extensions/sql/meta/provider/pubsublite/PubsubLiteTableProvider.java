package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoOneOf;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.DeadLetteredTransform;
import org.apache.beam.sdk.schemas.io.Failure;
import org.apache.beam.sdk.schemas.io.GenericDlq;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Pub/Sub Lite table provider.
 *
 * <p>Pub/Sub Lite tables may be constructed with:
 *
 * <pre>{@code
 * CREATE EXTERNAL TABLE tableName(
 *     message_key BYTES,  // optional
 *     publish_timestamp NOT NULL TIMESTAMP,  // optional
 *     event_timestamp TIMESTAMP,  // optional
 *     attributes ARRAY<ROW<key VARCHAR, values ARRAY<BYTES>>>,  // optional
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
      checkArgument(!properties.containsKey("format"), "Must not set the 'format' property if not unpacking payload.");
      return Optional.empty();
    }
    checkArgument(properties.containsKey("format"), "Must set the 'format' property if unpacking payload to a ROW.");
    return Optional.of(PayloadSerializers.getSerializer(properties.getString("format"), schema, properties.getInnerMap()));
  }

  private static void validateSchema(Schema schema) {
    checkArgument(schema.hasField(RowHandler.PAYLOAD_FIELD), "Must provide a 'payload' field for Pub/Sub Lite.");
    for (Field field : schema.getFields()) {
      switch (field.getName()) {
        case RowHandler.ATTRIBUTES_FIELD:
          checkArgument(field.getType().equals(RowHandler.ATTRIBUTES_FIELD_TYPE), String.format("'%s' field must have schema of exactly 'ARRAY<ROW<key VARCHAR, values ARRAY<BYTES>>>'.", field.getName()));
          break;
        case RowHandler.EVENT_TIMESTAMP_FIELD:
        case RowHandler.PUBLISH_TIMESTAMP_FIELD:
          checkArgument(field.getType().equals(FieldType.DATETIME), String.format("'%s' field must have schema of exactly 'TIMESTAMP'.", field.getName()));
          break;
        case RowHandler.MESSAGE_KEY_FIELD:
          checkArgument(field.getType().equals(FieldType.BYTES), String.format("'%s' field must have schema of exactly 'BYTES'.", field.getName()));
          break;
        case RowHandler.PAYLOAD_FIELD:
          checkArgument(field.getType().equals(FieldType.BYTES) || field.getType().getTypeName().equals(TypeName.ROW), String.format("'%s' field must either have a 'BYTES' or 'ROW' schema.", field.getName()));
          break;
        default:
          throw new IllegalArgumentException("'%s' field is invalid at the top level for Pub/Sub Lite.");
      }
    }
  }

  @AutoOneOf(Location.Kind.class)
  abstract static class Location {
    enum Kind {TOPIC, SUBSCRIPTION}
    abstract Kind getKind();

    abstract TopicPath topic();

    abstract SubscriptionPath subscription();

    static Location parse(String location) {
      if (location.contains("/topics/")) {
        return AutoValue_PubsubLiteTableProvider_Location.topic(TopicPath.parse(location));
      }
      if (location.contains("/subscriptions/")) {
        return AutoValue_PubsubLiteTableProvider_Location.subscription(SubscriptionPath.parse(location));
      }
      throw new IllegalArgumentException(String.format("Location '%s' does not correspond to either a Pub/Sub Lite topic or subscription.", location));
    }
  }

  private static RowHandler getRowHandler(Schema schema, Optional<PayloadSerializer> optionalSerializer) {
    if (optionalSerializer.isPresent()) {
      return new RowHandler(schema, optionalSerializer.get());
    }
    return new RowHandler(schema);
  }

  private static Optional<PTransform<PCollection<Failure>, PDone>> getDlqTransform(JSONObject properties) {
    if (!properties.containsKey("deadLetterQueue")) {
      return Optional.empty();
    }
    return Optional.of(GenericDlq.getDlqTransform(properties.getString("deadLetterQueue")));
  }

  private static <InputT, OutputT> PTransform<PCollection<? extends InputT>, PCollection<OutputT>> addDlqIfPresent(
      SerializableFunction<InputT, OutputT> transform,
      JSONObject properties) {
    if (properties.containsKey("deadLetterQueue")) {
      return new DeadLetteredTransform<>(transform, properties.getString("deadLetterQueue"));
    }
    return MapElements.into(new TypeDescriptor<OutputT>() {}).via(transform);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    validateSchema(table.getSchema());
    Optional<PayloadSerializer> serializer = getSerializer(table.getSchema(), table.getProperties());
    Location location = Location.parse(checkArgumentNotNull(table.getLocation()));
    RowHandler rowHandler = getRowHandler(table.getSchema(), serializer);
    Optional<PTransform<PCollection<Failure>, PDone>> dlq = getDlqTransform(table.getProperties());

    switch (location.getKind()) {
      case TOPIC:
        return new PubsubLiteTopicTable(table.getSchema(), location.topic(), addDlqIfPresent(rowHandler::rowToMessage, table.getProperties()));
      case SUBSCRIPTION:
        return new PubsubLiteSubscriptionTable(table.getSchema(), location.subscription(), addDlqIfPresent(rowHandler::messageToRow, table.getProperties()));
      default:
        throw new IllegalArgumentException("Invalid kind for location: " + location.getKind());
    }
  }
}
