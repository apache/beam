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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.FlatSchemaPubsubMessageToRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;

/** Read side converter for {@link PubsubMessage} with Avro payload. */
@Internal
@Experimental
@AutoValue
public abstract class AvroPubsubMessageToRow extends PubsubMessageToRow implements Serializable {

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple rows =
        input.apply(
            ParDo.of(new FlatSchemaAvroPubsubMessageToRow(messageSchema(), useDlq()))
                .withOutputTags(
                    MAIN_TAG, useDlq() ? TupleTagList.of(DLQ_TAG) : TupleTagList.empty()));
    rows.get(MAIN_TAG).setRowSchema(messageSchema());
    return rows;
  }

  public static Builder builder() {
    return new AutoValue_AvroPubsubMessageToRow.Builder();
  }

  @Internal
  private static class FlatSchemaAvroPubsubMessageToRow extends FlatSchemaPubsubMessageToRow {

    private final Schema messageSchema;

    private final boolean useDlq;

    protected FlatSchemaAvroPubsubMessageToRow(Schema messageSchema, boolean useDlq) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
    }

    @Override
    protected Schema getMessageSchema() {
      return messageSchema;
    }

    @Override
    protected boolean getUseDlq() {
      return useDlq;
    }

    @Override
    protected final Row parsePayload(PubsubMessage pubsubMessage) {
      byte[] avroPayload = pubsubMessage.getPayload();

      // Construct payload flat schema.
      Schema payloadSchema =
          new Schema(
              getMessageSchema().getFields().stream()
                  .filter(field -> !TIMESTAMP_FIELD.equals(field.getName()))
                  .collect(Collectors.toList()));
      org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(payloadSchema);
      return AvroUtils.toBeamRowStrict(AvroUtils.toGenericRecord(avroPayload, avroSchema), null);
    }
  }

  @Override
  public boolean useFlatSchema() {
    return true;
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder messageSchema(Schema messageSchema);

    public abstract Builder useDlq(boolean useDlq);

    public abstract AvroPubsubMessageToRow build();
  }
}
