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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link HadoopCatalog} that supports Iceberg v3 table encryption, for tests.
 *
 * <p>This catalog reproduces that wiring on top of {@link HadoopCatalog} so encryption can be
 * tested against a local warehouse. It builds an encryption manager from the table's {@code
 * encryption.key-id} property, and persists the keys it generates into the table metadata's {@code
 * encryption-keys} list on commit.
 *
 * <p>Enable it by pointing {@code catalog-impl} at this class and {@code encryption.kms-impl} at a
 * {@link KeyManagementClient} such as {@link TestKms}.
 */
public class EncryptedTestCatalog extends HadoopCatalog {
  private @Nullable KeyManagementClient kmsClient;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    this.kmsClient = EncryptionUtil.createKmsClient(properties);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    if (kmsClient == null) {
      return super.newTableOps(identifier);
    }
    return new EncryptingTableOperations(super.newTableOps(identifier), kmsClient);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (kmsClient != null) {
      kmsClient.close();
      kmsClient = null;
    }
  }

  /**
   * Delegating {@link TableOperations} that adds an encryption manager and carries the keys it
   * mints into table metadata.
   */
  private static class EncryptingTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final KeyManagementClient kmsClient;

    /**
     * Keys minted by encryption managers built by these operations. Kept across rebuilds so that a
     * key generated before a refresh is not lost before it is committed.
     */
    private final Map<String, EncryptedKey> knownKeys = new LinkedHashMap<>();

    private @Nullable EncryptionManager encryptionManager;
    private @Nullable FileIO encryptingFileIO;

    EncryptingTableOperations(TableOperations delegate, KeyManagementClient kmsClient) {
      this.delegate = delegate;
      this.kmsClient = kmsClient;
    }

    @Override
    public EncryptionManager encryption() {
      EncryptionManager existing = encryptionManager;
      if (existing != null) {
        return existing;
      }

      TableMetadata metadata = current();
      if (metadata == null) {
        // table does not exist yet; no properties to build an encryption manager from
        return PlaintextEncryptionManager.instance();
      }

      for (EncryptedKey key : metadata.encryptionKeys()) {
        knownKeys.putIfAbsent(key.keyId(), key);
      }
      EncryptionManager created =
          EncryptionUtil.createEncryptionManager(
              List.copyOf(knownKeys.values()), metadata.properties(), kmsClient);
      encryptionManager = created;
      return created;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      EncryptionManager encryption = encryption();
      TableMetadata toCommit = metadata;
      if (encryption instanceof StandardEncryptionManager) {
        knownKeys.putAll(EncryptionUtil.encryptionKeys(encryption));
        TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
        knownKeys.values().forEach(builder::addEncryptionKey);
        toCommit = builder.build();
      }

      delegate.commit(base, toCommit);
      this.encryptionManager = null;
      this.encryptingFileIO = null;
    }

    @Override
    public TableMetadata refresh() {
      TableMetadata refreshed = delegate.refresh();
      this.encryptionManager = null;
      this.encryptingFileIO = null;
      return refreshed;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    /**
     * Returns an {@link EncryptingFileIO} for encrypted tables, so manifests and manifest lists are
     * transparently decrypted. This mirrors {@code HiveTableOperations#io()}.
     */
    @Override
    public FileIO io() {
      EncryptionManager encryption = encryption();
      if (!(encryption instanceof StandardEncryptionManager)) {
        return delegate.io();
      }

      FileIO existing = encryptingFileIO;
      if (existing == null) {
        existing = EncryptingFileIO.combine(delegate.io(), encryption);
        encryptingFileIO = existing;
      }
      return existing;
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }
}
