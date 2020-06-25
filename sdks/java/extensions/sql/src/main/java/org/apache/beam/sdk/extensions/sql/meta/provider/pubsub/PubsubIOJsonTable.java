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

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * <i>Experimental</i>
 *
 * <p>Wraps the {@link PubsubIO} with JSON messages into {@link BeamSqlTable}.
 *
 * <p>This enables {@link PubsubIO} registration in Beam SQL environment as a table, including DDL
 * support.
 *
 * <p>Pubsub messages include metadata along with the payload, and it has to be explicitly specified
 * in the schema to make sure it is available to the queries.
 *
 * <p>The fields included in the Pubsub message model are: 'event_timestamp', 'attributes', and
 * 'payload'.
 *
 * <p>For example:
 *
 * <p>If the messages have JSON messages in the payload that look like this:
 *
 * <pre>
 *  {
 *    "id" : 5,
 *    "name" : "foo"
 *  }
 * </pre>
 *
 * <p>Then SQL statements to declare and query such a topic will look like this:
 *
 * <pre>
 *  CREATE TABLE topic_table (
 *        event_timestamp TIMESTAMP,
 *        attributes MAP&lt;VARCHAR, VARCHAR&gt;,
 *        payload ROW&lt;name VARCHAR, age INTEGER&gt;
 *      )
 *     TYPE 'pubsub'
 *     LOCATION projects/&lt;GCP project id&gt;/topics/&lt;topic name&gt;
 *     TBLPROPERTIES '{ \"timestampAttributeKey\" : &lt;timestamp attribute&gt; }';
 *
 *  SELECT event_timestamp, topic_table.payload.name FROM topic_table;
 * </pre>
 *
 * <p>Note, 'payload' field is defined as ROW with schema matching the JSON payload of the message.
 * If 'timestampAttributeKey' is specified in TBLPROPERTIES then 'event_timestamp' will be set to
 * the value of that attribute. If it is not specified, then message publish time will be used as
 * event timestamp. 'attributes' map contains Pubsub message attributes map unchanged and can be
 * referenced in the queries as well.
 *
 * <p>In this configuration, only {@code event_timestamp} is required to be specified in the table
 * schema. All other fields are assumed to be part of the message payload. SQL statements to declare
 * and query the same topic as above will look like this:
 *
 * <pre>
 *  CREATE TABLE topic_table (
 *        event_timestamp TIMESTAMP,
 *        name VARCHAR,
 *        age INTEGER
 *     )
 *     TYPE 'pubsub'
 *     LOCATION projects/&lt;GCP project id&gt;/topics/&lt;topic name&gt;
 *     TBLPROPERTIES '{ \"timestampAttributeKey\" : &lt;timestamp attribute&gt; }';
 *
 *  SELECT event_timestamp, name FROM topic_table;
 * </pre>
 *
 * <p>If 'timestampAttributeKey' is specified in TBLPROPERTIES then 'event_timestamp' will be set to
 * the value of that attribute. If it is not specified, then message publish time will be used as
 * event timestamp.
 *
 * <p>In order to write to the same table you can use an INSERT statement like this:
 *
 * <pre>
 *   INSERT INTO topic_table VALUES (TIMESTAMP '2019-11-13 10:14:14', 'Brian', 30)
 * </pre>
 *
 * <p>Note that when writing, the value for {@code event_timestamp} is ignored by default, since the
 * Pubsub-managed publish time will be used to populate {@code event_timestamp} on read. In order to
 * ensure the {@code event_timestamp} you specified is used, you should specify
 * 'timestampAttributeKey' in TBLPROPERTIES.
 */
@Internal
@Experimental
class PubsubIOJsonTable extends BaseBeamTable implements Serializable {

  protected final PubsubSchemaIO pubsubSchemaIO;
  protected final Schema schema;

  private PubsubIOJsonTable(PubsubSchemaIO pubsubSchemaIO, Schema schema) {
    this.pubsubSchemaIO = pubsubSchemaIO;
    this.schema = schema;
  }

  static PubsubIOJsonTable withConfiguration(PubsubSchemaIO pubsubSchemaIO, Schema schema) {
    return new PubsubIOJsonTable(pubsubSchemaIO, schema);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    PTransform<PBegin, PCollection<Row>> readerTransform = pubsubSchemaIO.buildReader();
    return readerTransform.expand(begin);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    PTransform<PCollection<Row>, POutput> writerTransform = pubsubSchemaIO.buildWriter();
    return writerTransform.expand(input);
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }
}
