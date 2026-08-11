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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * A serializable, lightweight representation of an Iceberg {@link Table}'s declarative metadata.
 *
 * <p>Captures the table's schema, partition spec, sort order, location, properties, and identifier.
 * Suitable for broadcasting across worker nodes via Beam's side-input mechanism.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SerializableTableSpec implements Serializable {

  @SchemaFieldNumber("0")
  public abstract String getTableIdentifierString();

  @SchemaFieldNumber("1")
  public abstract String getName();

  @SchemaFieldNumber("2")
  public abstract String getLocation();

  @SchemaFieldNumber("3")
  public abstract int getSpecId();

  @SchemaFieldNumber("4")
  public abstract String getSchemaJson();

  @SchemaFieldNumber("5")
  public abstract String getPartitionSpecJson();

  @SchemaFieldNumber("6")
  public abstract String getSortOrderJson();

  @SchemaFieldNumber("7")
  public abstract Map<String, String> getProperties();

  private transient volatile @MonotonicNonNull Schema cachedSchema;
  private transient volatile @MonotonicNonNull PartitionSpec cachedPartitionSpec;
  private transient volatile @MonotonicNonNull SortOrder cachedSortOrder;
  private transient volatile @MonotonicNonNull TableIdentifier cachedTableIdentifier;

  private static volatile @MonotonicNonNull SchemaCoder<SerializableTableSpec> cachedCoder;

  @SchemaIgnore
  public Schema getSchema() {
    Schema local = cachedSchema;
    if (local == null) {
      synchronized (this) {
        local = cachedSchema;
        if (local == null) {
          cachedSchema = local = SchemaParser.fromJson(getSchemaJson());
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public PartitionSpec getPartitionSpec() {
    PartitionSpec local = cachedPartitionSpec;
    if (local == null) {
      synchronized (this) {
        local = cachedPartitionSpec;
        if (local == null) {
          cachedPartitionSpec =
              local = PartitionSpecParser.fromJson(getSchema(), getPartitionSpecJson());
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public SortOrder getSortOrder() {
    SortOrder local = cachedSortOrder;
    if (local == null) {
      synchronized (this) {
        local = cachedSortOrder;
        if (local == null) {
          cachedSortOrder = local = SortOrderParser.fromJson(getSchema(), getSortOrderJson());
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public TableIdentifier getTableIdentifier() {
    TableIdentifier local = cachedTableIdentifier;
    if (local == null) {
      synchronized (this) {
        local = cachedTableIdentifier;
        if (local == null) {
          cachedTableIdentifier =
              local = IcebergUtils.parseTableIdentifier(getTableIdentifierString());
        }
      }
    }
    return local;
  }

  public static Builder builder() {
    return new AutoValue_SerializableTableSpec.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableIdentifierString(String tableIdentifierString);

    public abstract Builder setName(String name);

    public abstract Builder setLocation(String location);

    public abstract Builder setSpecId(int specId);

    public abstract Builder setSchemaJson(String schemaJson);

    public abstract Builder setPartitionSpecJson(String partitionSpecJson);

    public abstract Builder setSortOrderJson(String sortOrderJson);

    public abstract Builder setProperties(Map<String, String> properties);

    public abstract SerializableTableSpec build();
  }

  /**
   * Constructs a {@link SerializableTableSpec} from a {@link Table}, using {@link Table#name()} as
   * the table identifier string.
   *
   * <p>Note: When possible, prefer {@link #fromTable(TableIdentifier, Table)} to avoid catalog name
   * prefix ambiguities in {@link Table#name()}.
   */
  public static SerializableTableSpec fromTable(Table table) {
    return fromTable(table.name(), table);
  }

  /**
   * Constructs a {@link SerializableTableSpec} from a {@link TableIdentifier} and a {@link Table}.
   */
  public static SerializableTableSpec fromTable(TableIdentifier tableIdentifier, Table table) {
    return fromTable(IcebergUtils.tableIdentifierToString(tableIdentifier), table);
  }

  /**
   * Constructs a {@link SerializableTableSpec} from an explicit table identifier string and a
   * {@link Table}.
   */
  public static SerializableTableSpec fromTable(String tableIdentifierString, Table table) {
    return builder()
        .setTableIdentifierString(tableIdentifierString)
        .setName(table.name())
        .setLocation(table.location())
        .setSpecId(table.spec().specId())
        .setSchemaJson(SchemaParser.toJson(table.schema()))
        .setPartitionSpecJson(PartitionSpecParser.toJson(table.spec()))
        .setSortOrderJson(SortOrderParser.toJson(table.sortOrder()))
        .setProperties(table.properties())
        .build();
  }

  /** Returns the cached {@link SchemaCoder} for {@link SerializableTableSpec}. */
  public static SchemaCoder<SerializableTableSpec> getCoder() {
    if (cachedCoder == null) {
      synchronized (SerializableTableSpec.class) {
        if (cachedCoder == null) {
          try {
            cachedCoder =
                SchemaRegistry.createDefault().getSchemaCoder(SerializableTableSpec.class);
          } catch (NoSuchSchemaException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return checkStateNotNull(cachedCoder);
  }
}
