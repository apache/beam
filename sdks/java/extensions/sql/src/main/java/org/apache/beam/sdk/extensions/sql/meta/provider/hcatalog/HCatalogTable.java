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
package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * <i>Experimental</i>
 *
 * <p>Beam SQL table that wraps {@link HCatalogIO}.
 *
 * <p>Reads {@link org.apache.hive.hcatalog.data.HCatRecord HCatRecords} and converts them to {@link
 * Row Rows}.
 */
@AutoValue
@Experimental
public abstract class HCatalogTable implements BeamSqlTable {

  public abstract Schema schema();

  public abstract Map<String, String> config();

  public abstract String database();

  public abstract String table();

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(
            "HCatalog-Read-" + database() + "-" + table(),
            HCatalogIO.read()
                .withConfigProperties(config())
                .withDatabase(database())
                .withTable(table()))

        // Converting to Rows is done here instead of using the schema provider
        // with SchemaRegistry. Reason is that schema lookup in SchemaRegistry
        // Happens via TypeDescriptor of the element, so for all HCatRecords from all sources
        // the schema is either expected to be the same.
        //
        // As a workaround we could do a run-time schema conversion per-record but that's
        // not efficient.
        .apply("HCatRecord-to-Row", SchemaUtils.toRow(schema()))
        .setRowSchema(schema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("Writing to HCatalog is not supported in Beam SQL");
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public Schema getSchema() {
    return schema();
  }

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_HCatalogTable.Builder();
  }

  /** Javadoc comment. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setSchema(Schema schema);

    abstract Builder setConfig(Map<String, String> config);

    abstract Builder setDatabase(String database);

    abstract Builder setTable(String table);

    abstract HCatalogTable build();
  }
}
