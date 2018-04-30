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

import com.alibaba.fastjson.JSONObject;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.Schema;

/**
 * {@link TableProvider} for {@link PubsubIOJsonTable} which wraps {@link PubsubIO} for Beam SQL.
 */
@Internal
@Experimental
public class PubsubJsonTableProvider extends InMemoryMetaTableProvider {

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema tableSchema = table.getSchema();

    JSONObject tableProperties = table.getProperties();
    String timestampAttributeKey = tableProperties.getString("timestampAttributeKey");

    if (timestampAttributeKey == null) {
      throw new IllegalArgumentException(
          "Unable to find 'timestampAttributeKey' property "
          + "in TBLPROPERTIES JSON. At the moment Pubsub table "
          + "have to explicitly declare the 'timestampAttributeKey', "
          + "so that event time can be determined. Publish time "
          + "or other timestamp sources are not supported at this time.");
    }

    if (tableSchema.hasField(timestampAttributeKey)) {
      throw new IllegalArgumentException(
          "Conflicting field '" + timestampAttributeKey + "'. "
          + "It is declared both in the table schema and as a timestamp attribute "
          + "at the same time. This is not supported. Either remove field from table schema "
          + "or choose a different timestamp attribute.");
    }

    return
        PubsubIOJsonTable
            .builder()
            .setPayloadSchema(tableSchema)
            .setTimestampAttribute(timestampAttributeKey)
            .setTopic(table.getLocation())
            .build();
  }

  @Override
  public String getTableType() {
    return "pubsub";
  }
}
