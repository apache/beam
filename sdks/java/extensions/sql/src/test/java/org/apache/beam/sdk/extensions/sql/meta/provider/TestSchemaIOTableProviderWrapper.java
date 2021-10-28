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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.PushdownProjector;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A mock {@link org.apache.beam.sdk.extensions.sql.meta.provider.SchemaIOTableProviderWrapper} that
 * reads in-memory data for testing.
 */
public class TestSchemaIOTableProviderWrapper extends SchemaIOTableProviderWrapper {
  private static final List<Row> rows = new ArrayList<>();

  @Override
  public SchemaIOProvider getSchemaIOProvider() {
    return new TestSchemaIOProvider();
  }

  public static void addRows(Row... newRows) {
    rows.addAll(Arrays.asList(newRows));
  }

  private class TestSchemaIOProvider implements SchemaIOProvider {
    @Override
    public String identifier() {
      return "TestSchemaIOProvider";
    }

    @Override
    public Schema configurationSchema() {
      return Schema.of();
    }

    @Override
    public SchemaIO from(String location, Row configuration, @Nullable Schema dataSchema) {
      return new TestSchemaIO(dataSchema);
    }

    @Override
    public boolean requiresDataSchema() {
      return true;
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }
  }

  private class TestSchemaIO implements SchemaIO {
    private final Schema schema;

    TestSchemaIO(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      // Read all fields by default.
      return new TestPushdownProjector(schema, FieldAccessDescriptor.withAllFields());
    }

    @Override
    public PTransform<PCollection<Row>, ? extends POutput> buildWriter() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * {@link PTransform} that reads in-memory data for testing. Simulates projection pushdown using
   * {@link Select}.
   */
  private class TestPushdownProjector extends PTransform<PBegin, PCollection<Row>>
      implements PushdownProjector<PBegin> {
    /** The schema of the input data. */
    private final Schema schema;
    /** The fields to be projected. */
    private final FieldAccessDescriptor fieldAccessDescriptor;

    TestPushdownProjector(Schema schema, FieldAccessDescriptor fieldAccessDescriptor) {
      this.schema = schema;
      this.fieldAccessDescriptor = fieldAccessDescriptor;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> withProjectionPushdown(
        FieldAccessDescriptor fieldAccessDescriptor) {
      return new TestPushdownProjector(schema, fieldAccessDescriptor);
    }

    @Override
    public boolean supportsFieldReordering() {
      return true;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      // Simulate projection pushdown using Select. In a real IO, projection would be pushed down to
      // the source.
      return input
          .apply(Create.of(rows).withRowSchema(schema))
          .apply(Select.fieldAccess(fieldAccessDescriptor));
    }
  }
}
