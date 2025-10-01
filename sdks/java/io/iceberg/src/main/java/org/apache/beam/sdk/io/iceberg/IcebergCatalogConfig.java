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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class IcebergCatalogConfig implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfig.class);
  private transient @MonotonicNonNull Catalog cachedCatalog;

  @Pure
  @Nullable
  public abstract String getCatalogName();

  @Pure
  @Nullable
  public abstract Map<String, String> getCatalogProperties();

  @Pure
  @Nullable
  public abstract Map<String, String> getConfigProperties();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergCatalogConfig.Builder();
  }

  public abstract Builder toBuilder();

  public org.apache.iceberg.catalog.Catalog catalog() {
    if (cachedCatalog == null) {
      String catalogName = getCatalogName();
      if (catalogName == null) {
        catalogName = "apache-beam-" + ReleaseInfo.getReleaseInfo().getVersion();
      }
      Map<String, String> catalogProps = getCatalogProperties();
      if (catalogProps == null) {
        catalogProps = Maps.newHashMap();
      }
      Map<String, String> confProps = getConfigProperties();
      if (confProps == null) {
        confProps = Maps.newHashMap();
      }
      Configuration config = new Configuration();
      for (Map.Entry<String, String> prop : confProps.entrySet()) {
        config.set(prop.getKey(), prop.getValue());
      }
      cachedCatalog = CatalogUtil.buildIcebergCatalog(catalogName, catalogProps, config);
    }
    return cachedCatalog;
  }

  private void checkSupportsNamespaces() {
    Preconditions.checkState(
        catalog() instanceof SupportsNamespaces,
        "Catalog '%s' does not support handling namespaces.",
        catalog().name());
  }

  public boolean createNamespace(String namespace) {
    checkSupportsNamespaces();
    String[] components = Iterables.toArray(Splitter.on('.').split(namespace), String.class);

    try {
      ((SupportsNamespaces) catalog()).createNamespace(Namespace.of(components));
      return true;
    } catch (AlreadyExistsException e) {
      return false;
    }
  }

  public Set<String> listNamespaces() {
    checkSupportsNamespaces();

    return ((SupportsNamespaces) catalog())
        .listNamespaces().stream().map(Namespace::toString).collect(Collectors.toSet());
  }

  public boolean dropNamespace(String namespace, boolean cascade) {
    checkSupportsNamespaces();

    String[] components = Iterables.toArray(Splitter.on('.').split(namespace), String.class);
    Namespace ns = Namespace.of(components);

    if (!((SupportsNamespaces) catalog()).namespaceExists(ns)) {
      return false;
    }

    // Cascade will delete all contained tables first
    if (cascade) {
      catalog().listTables(ns).forEach(catalog()::dropTable);
    }

    // Drop the namespace
    return ((SupportsNamespaces) catalog()).dropNamespace(Namespace.of(components));
  }

  public void createTable(
      String tableIdentifier, Schema tableSchema, @Nullable List<String> partitionFields) {
    TableIdentifier icebergIdentifier = TableIdentifier.parse(tableIdentifier);
    org.apache.iceberg.Schema icebergSchema = IcebergUtils.beamSchemaToIcebergSchema(tableSchema);
    PartitionSpec icebergSpec = PartitionUtils.toPartitionSpec(partitionFields, tableSchema);
    try {
      catalog().createTable(icebergIdentifier, icebergSchema, icebergSpec);
      LOG.info(
          "Created table '{}' with schema: {}\n, partition spec: {}",
          icebergIdentifier,
          icebergSchema,
          icebergSpec);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(e);
    }
  }

  public boolean dropTable(String tableIdentifier) {
    TableIdentifier icebergIdentifier = TableIdentifier.parse(tableIdentifier);
    return catalog().dropTable(icebergIdentifier);
  }

  public Set<String> listTables(String namespace) {
    return catalog().listTables(Namespace.of(namespace)).stream()
        .map(TableIdentifier::name)
        .collect(Collectors.toSet());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCatalogName(@Nullable String catalogName);

    public abstract Builder setCatalogProperties(@Nullable Map<String, String> props);

    public abstract Builder setConfigProperties(@Nullable Map<String, String> props);

    public abstract IcebergCatalogConfig build();
  }
}
