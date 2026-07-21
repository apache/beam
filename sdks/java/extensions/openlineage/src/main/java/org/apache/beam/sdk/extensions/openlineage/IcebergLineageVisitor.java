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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts dataset identities from {@code IcebergIO.ReadRows} and {@code IcebergIO.WriteRows},
 * following the Spark integration's {@code IcebergHandler}: the primary identity is the table's
 * physical location (via {@link FilesystemDatasetUtils}, so object-store naming matches the
 * OpenLineage spec) and the catalog table identity is attached as a {@code TABLE} symlink. When the
 * catalog cannot be reached at submission time, falls back to a catalog-based {@code iceberg://}
 * identity so the dataset is still reported.
 *
 * <p>IcebergIO's configuration getters are package-private, so they are read reflectively; this
 * visitor is only instantiated when IcebergIO is on the classpath (see {@link VisitorFactory}).
 */
class IcebergLineageVisitor extends PipelineLineageVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergLineageVisitor.class);
  private static final String ICEBERG_WRITE_CLASS =
      "org.apache.beam.sdk.io.iceberg.IcebergIO$WriteRows";
  private static final String ICEBERG_READ_CLASS =
      "org.apache.beam.sdk.io.iceberg.IcebergIO$ReadRows";

  @Override
  boolean isDefinedAt(PTransform<?, ?> transform) {
    return extendsClass(transform, ICEBERG_WRITE_CLASS)
        || extendsClass(transform, ICEBERG_READ_CLASS);
  }

  @Override
  List<DatasetIdentifier> applyInputs(PTransform<?, ?> transform) {
    if (!extendsClass(transform, ICEBERG_READ_CLASS)) {
      return Collections.emptyList();
    }
    return extract(transform);
  }

  @Override
  List<DatasetIdentifier> applyOutputs(PTransform<?, ?> transform) {
    if (!extendsClass(transform, ICEBERG_WRITE_CLASS)) {
      return Collections.emptyList();
    }
    return extract(transform);
  }

  private List<DatasetIdentifier> extract(PTransform<?, ?> transform) {
    try {
      Object catalogConfig = invokeDeclared(transform, "getCatalogConfig");
      Object tid = invokeDeclared(transform, "getTableIdentifier");
      if (catalogConfig == null || tid == null) {
        // Dynamic destinations: the table is not known until runtime.
        LOG.info("IcebergIO transform without a static table identifier; skipping");
        return Collections.emptyList();
      }
      TableIdentifier tableId =
          tid instanceof TableIdentifier
              ? (TableIdentifier) tid
              : TableIdentifier.parse(tid.toString());
      String catalogName = (String) invokeDeclared(catalogConfig, "getCatalogName");
      @SuppressWarnings("unchecked")
      Map<String, String> properties =
          (Map<String, String>) invokeDeclared(catalogConfig, "getCatalogProperties");
      String uri = properties == null ? null : properties.get("uri");
      String warehouse = properties == null ? null : properties.get("warehouse");

      String symlinkNamespace =
          uri != null ? uri : (warehouse != null ? warehouse : "iceberg://" + catalogName);
      String location = loadTableLocation(catalogConfig, tableId);
      DatasetIdentifier identifier;
      if (location != null) {
        identifier = FilesystemDatasetUtils.fromLocation(URI.create(location));
      } else {
        identifier =
            new DatasetIdentifier(tableId.toString(), "iceberg://" + authority(uri, catalogName));
      }
      return Collections.singletonList(
          identifier.withSymlink(
              tableId.toString(), symlinkNamespace, DatasetIdentifier.SymlinkType.TABLE));
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.warn("Unable to extract lineage from IcebergIO transform", e);
      return Collections.emptyList();
    }
  }

  /** Loads the table's physical location from the catalog; null when unreachable. */
  private static @Nullable String loadTableLocation(Object catalogConfig, TableIdentifier tableId) {
    try {
      Object catalog = invokeDeclared(catalogConfig, "catalog");
      if (catalog instanceof Catalog) {
        return ((Catalog) catalog).loadTable(tableId).location();
      }
    } catch (ReflectiveOperationException | RuntimeException | NoClassDefFoundError e) {
      LOG.info(
          "Could not resolve Iceberg table location for {} (catalog unreachable at submit "
              + "time?): {}",
          tableId,
          e.toString());
    }
    return null;
  }

  private static String authority(@Nullable String uri, @Nullable String catalogName) {
    if (uri != null) {
      try {
        String authority = URI.create(uri).getAuthority();
        if (authority != null) {
          return authority;
        }
      } catch (RuntimeException e) {
        // fall through to the catalog name
      }
    }
    return catalogName != null ? catalogName : "unknown";
  }

  private static @Nullable Object invokeDeclared(Object target, String methodName)
      throws ReflectiveOperationException {
    Class<?> cls = target.getClass();
    while (cls != null) {
      try {
        Method method = cls.getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
      } catch (NoSuchMethodException e) {
        cls = cls.getSuperclass();
      }
    }
    return null;
  }
}
