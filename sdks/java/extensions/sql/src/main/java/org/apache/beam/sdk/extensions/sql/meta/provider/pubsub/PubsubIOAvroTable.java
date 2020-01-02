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

import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.MAIN_TAG;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider.PubsubIOTableConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

@Internal
@Experimental
class PubsubIOAvroTable extends PubsubIOTable implements Serializable {

  private final PubsubIOTableConfiguration config;

  private PubsubIOAvroTable(PubsubIOTableConfiguration config) {
    this.config = config;
  }

  @Override
  protected PubsubIOTableConfiguration getConfig() {
    return config;
  }

  static PubsubIOAvroTable withConfiguration(PubsubIOTableConfiguration config) {
    return new PubsubIOAvroTable(config);
  }

  @Override
  public Schema getSchema() {
    return config.getSchema();
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    PCollectionTuple rowsWithDlq =
        begin
            .apply("ReadFromPubsub", readMessagesWithAttributes())
            .apply(
                "AvroPubsubMessageToRow",
                AvroPubsubMessageToRow.builder()
                    .messageSchema(getSchema())
                    .useDlq(config.useDlq())
                    .build());

    if (config.useDlq()) {
      rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
    }

    return rowsWithDlq.get(MAIN_TAG);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    if (!config.getUseFlatSchema()) {
      throw new UnsupportedOperationException(
          "Writing to a Pubsub topic is only supported for flattened schemas");
    }
    return input
        .apply(RowToAvroPubsubMessage.fromTableConfig(config))
        .apply(createPubsubMessageWrite());
  }
}
