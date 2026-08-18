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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.EncryptedKeyParser;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A serializable, lightweight representation of an Iceberg {@link Table}'s declarative metadata.
 *
 * <p>Captures the table's schemas, partition specs, sort orders, location, properties, identifier,
 * encrypted keys, and serialized {@link FileIO} configuration. Suitable for broadcasting across
 * worker nodes via Beam's side-input mechanism.
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
  public abstract int getSchemaId();

  @SchemaFieldNumber("4")
  public abstract Map<Integer, String> getSchemasJson();

  @SchemaFieldNumber("5")
  public abstract int getSpecId();

  @SchemaFieldNumber("6")
  public abstract Map<Integer, String> getPartitionSpecsJson();

  @SchemaFieldNumber("7")
  public abstract int getOrderId();

  @SchemaFieldNumber("8")
  public abstract Map<Integer, String> getSortOrdersJson();

  @SchemaFieldNumber("9")
  public abstract Map<String, String> getProperties();

  @SchemaFieldNumber("10")
  public abstract String getFileIoJson();

  @SchemaFieldNumber("11")
  public abstract List<String> getEncryptedKeyJsons();

  private transient volatile @MonotonicNonNull Map<Integer, Schema> cachedSchemas;
  private transient volatile @MonotonicNonNull Map<Integer, PartitionSpec> cachedPartitionSpecs;
  private transient volatile @MonotonicNonNull Map<Integer, SortOrder> cachedSortOrders;
  private transient volatile @MonotonicNonNull TableIdentifier cachedTableIdentifier;
  private transient volatile @MonotonicNonNull FileIO cachedFileIO;
  private transient volatile @MonotonicNonNull List<EncryptedKey> cachedEncryptedKeys;

  private static volatile @MonotonicNonNull SchemaCoder<SerializableTableSpec> cachedCoder;

  @SchemaIgnore
  public Map<Integer, Schema> getSchemas() {
    Map<Integer, Schema> local = cachedSchemas;
    if (local == null) {
      synchronized (this) {
        local = cachedSchemas;
        if (local == null) {
          ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
          for (Map.Entry<Integer, String> entry : getSchemasJson().entrySet()) {
            builder.put(entry.getKey(), SchemaParser.fromJson(entry.getValue()));
          }
          cachedSchemas = local = builder.build();
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public Schema getSchema() {
    Schema schema = getSchemas().get(getSchemaId());
    if (schema == null) {
      throw new IllegalStateException(
          "Schema with id " + getSchemaId() + " not found in schemas map");
    }
    return schema;
  }

  @SchemaIgnore
  public @Nullable Schema getSchema(int schemaId) {
    return getSchemas().get(schemaId);
  }

  @SchemaIgnore
  public Map<Integer, PartitionSpec> getPartitionSpecs() {
    Map<Integer, PartitionSpec> local = cachedPartitionSpecs;
    if (local == null) {
      synchronized (this) {
        local = cachedPartitionSpecs;
        if (local == null) {
          ImmutableMap.Builder<Integer, PartitionSpec> builder = ImmutableMap.builder();
          for (Map.Entry<Integer, String> entry : getPartitionSpecsJson().entrySet()) {
            builder.put(
                entry.getKey(), PartitionSpecParser.fromJson(getSchema(), entry.getValue()));
          }
          cachedPartitionSpecs = local = builder.build();
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public PartitionSpec getPartitionSpec() {
    PartitionSpec spec = getPartitionSpecs().get(getSpecId());
    if (spec == null) {
      throw new IllegalStateException(
          "PartitionSpec with id " + getSpecId() + " not found in partitionSpecs map");
    }
    return spec;
  }

  @SchemaIgnore
  public @Nullable PartitionSpec getPartitionSpec(int specId) {
    return getPartitionSpecs().get(specId);
  }

  @SchemaIgnore
  public Map<Integer, SortOrder> getSortOrders() {
    Map<Integer, SortOrder> local = cachedSortOrders;
    if (local == null) {
      synchronized (this) {
        local = cachedSortOrders;
        if (local == null) {
          ImmutableMap.Builder<Integer, SortOrder> builder = ImmutableMap.builder();
          for (Map.Entry<Integer, String> entry : getSortOrdersJson().entrySet()) {
            builder.put(entry.getKey(), SortOrderParser.fromJson(getSchema(), entry.getValue()));
          }
          cachedSortOrders = local = builder.build();
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public SortOrder getSortOrder() {
    SortOrder order = getSortOrders().get(getOrderId());
    if (order == null) {
      throw new IllegalStateException(
          "SortOrder with id " + getOrderId() + " not found in sortOrders map");
    }
    return order;
  }

  @SchemaIgnore
  public @Nullable SortOrder getSortOrder(int orderId) {
    return getSortOrders().get(orderId);
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

  @SchemaIgnore
  public FileIO getFileIO() {
    FileIO local = cachedFileIO;
    if (local == null) {
      synchronized (this) {
        local = cachedFileIO;
        if (local == null) {
          cachedFileIO = local = FileIOParser.fromJson(getFileIoJson());
        }
      }
    }
    return local;
  }

  @SchemaIgnore
  public List<EncryptedKey> getEncryptedKeys() {
    List<EncryptedKey> local = cachedEncryptedKeys;
    if (local == null) {
      synchronized (this) {
        local = cachedEncryptedKeys;
        if (local == null) {
          cachedEncryptedKeys =
              local =
                  getEncryptedKeyJsons().stream()
                      .map(EncryptedKeyParser::fromJson)
                      .collect(Collectors.toList());
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

    public abstract Builder setSchemaId(int schemaId);

    public abstract Builder setSchemasJson(Map<Integer, String> schemasJson);

    public abstract Builder setSpecId(int specId);

    public abstract Builder setPartitionSpecsJson(Map<Integer, String> partitionSpecsJson);

    public abstract Builder setOrderId(int orderId);

    public abstract Builder setSortOrdersJson(Map<Integer, String> sortOrdersJson);

    public abstract Builder setProperties(Map<String, String> properties);

    public abstract Builder setFileIoJson(String fileIoJson);

    public abstract Builder setEncryptedKeyJsons(List<String> encryptedKeyJsons);

    @SchemaIgnore
    public Builder setFileIO(FileIO fileIO) {
      return setFileIoJson(FileIOParser.toJson(fileIO));
    }

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
    if (!(table instanceof HasTableOperations)) {
      throw new IllegalArgumentException(
          String.format(
              "Table %s of class %s does not implement HasTableOperations",
              table.name(), table.getClass().getName()));
    }

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    List<String> encryptedKeyJsons = Collections.emptyList();
    if (metadata != null && metadata.encryptionKeys() != null) {
      encryptedKeyJsons =
          metadata.encryptionKeys().stream()
              .map(key -> EncryptedKeyParser.toJson(key, false))
              .collect(Collectors.toList());
    }

    ImmutableMap.Builder<Integer, String> schemasJson = ImmutableMap.builder();
    for (Map.Entry<Integer, Schema> entry : table.schemas().entrySet()) {
      schemasJson.put(entry.getKey(), SchemaParser.toJson(entry.getValue()));
    }

    ImmutableMap.Builder<Integer, String> specsJson = ImmutableMap.builder();
    for (Map.Entry<Integer, PartitionSpec> entry : table.specs().entrySet()) {
      specsJson.put(entry.getKey(), PartitionSpecParser.toJson(entry.getValue()));
    }

    ImmutableMap.Builder<Integer, String> sortOrdersJson = ImmutableMap.builder();
    for (Map.Entry<Integer, SortOrder> entry : table.sortOrders().entrySet()) {
      sortOrdersJson.put(entry.getKey(), SortOrderParser.toJson(entry.getValue()));
    }

    return builder()
        .setTableIdentifierString(tableIdentifierString)
        .setName(table.name())
        .setLocation(table.location())
        .setSchemaId(table.schema().schemaId())
        .setSchemasJson(schemasJson.build())
        .setSpecId(table.spec().specId())
        .setPartitionSpecsJson(specsJson.build())
        .setOrderId(table.sortOrder().orderId())
        .setSortOrdersJson(sortOrdersJson.build())
        .setProperties(table.properties())
        .setFileIoJson(FileIOParser.toJson(table.io()))
        .setEncryptedKeyJsons(encryptedKeyJsons)
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
