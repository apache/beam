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
package org.apache.beam.sdk.io.gcp.datastore;

import com.google.auto.service.AutoService;
import com.google.datastore.v1.Query;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidLocationException;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing payloads with {@link
 * DatastoreIO}.
 */
@Internal
@AutoService(SchemaIOProvider.class)
public class DataStoreV1SchemaIOProvider implements SchemaIOProvider {
  public static final String KEY_FIELD_PROPERTY = "keyField";
  static final String DEFAULT_KEY_FIELD = "__key__";
  private static final Pattern locationPattern = Pattern.compile("(?<projectId>.+)/(?<kind>.+)");

  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "datastoreV1";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself.
   *
   * <p>Configuration Parameters:
   *
   * <ul>
   *   <li>STRING keyField: The name of the Beam schema field to map the DataStore entity key.
   *       Defaults to {@code __key__} if not set or null.
   * </ul>
   */
  @Override
  public Schema configurationSchema() {
    // TODO: allow users to specify a name of the field to store a key value via TableProperties.
    return Schema.builder().addNullableField(KEY_FIELD_PROPERTY, Schema.FieldType.STRING).build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public DataStoreV1SchemaIO from(String location, Row configuration, Schema dataSchema) {
    return new DataStoreV1SchemaIO(location, configuration, dataSchema);
  }

  @Override
  public boolean requiresDataSchema() {
    return true;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  /** An abstraction to create schema aware IOs. */
  public static class DataStoreV1SchemaIO implements SchemaIO, Serializable {
    protected final Schema dataSchema;
    protected final String location;
    protected final String kind;
    protected final String projectId;
    protected final String keyField;

    private DataStoreV1SchemaIO(String location, Row config, Schema dataSchema) {
      this.location = location;
      this.dataSchema = dataSchema;
      this.keyField = determineKeyField(config.getString(KEY_FIELD_PROPERTY));

      Matcher matcher = locationPattern.matcher(this.location);
      validateLocation(location, matcher);

      this.kind = matcher.group("kind");
      this.projectId = matcher.group("projectId");
    }

    @Override
    public Schema schema() {
      return dataSchema;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin begin) {
          Query.Builder q = Query.newBuilder();
          q.addKindBuilder().setName(kind);
          Query query = q.build();

          DatastoreV1.Read readInstance =
              DatastoreIO.v1().read().withProjectId(projectId).withQuery(query);

          return begin
              .apply("Read Datastore Entities", readInstance)
              .apply(
                  "Convert Datastore Entities to Rows", EntityToRow.create(dataSchema, keyField));
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, POutput> buildWriter() {
      return new PTransform<PCollection<Row>, POutput>() {
        @Override
        public POutput expand(PCollection<Row> input) {
          return input
              .apply("Convert Rows to Datastore Entities", RowToEntity.create(keyField, kind))
              .apply("Write Datastore Entities", DatastoreIO.v1().write().withProjectId(projectId));
        }
      };
    }

    public String getProjectId() {
      return projectId;
    }

    public String getKind() {
      return kind;
    }

    private String determineKeyField(String configKey) {
      if (configKey != null && configKey.isEmpty()) {
        throw new InvalidConfigurationException(
            String.format("'%s' property cannot be null.", KEY_FIELD_PROPERTY));
      } else if (configKey != null) {
        return configKey;
      } else {
        return DEFAULT_KEY_FIELD;
      }
    }

    private void validateLocation(String location, Matcher matcher) {
      // TODO: allow users to specify a namespace in a location string.
      if (location == null) {
        throw new InvalidLocationException("DataStoreV1 location must be set. ");
      }
      if (!matcher.matches()) {
        throw new InvalidLocationException(
            "DataStoreV1 location must be in the following format: 'projectId/kind'"
                + " but was:"
                + location);
      }
    }
  }
}
