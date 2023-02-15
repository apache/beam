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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.threeten.bp.Duration;

/**
 * Factory class that caches {@link BigtableService} to share between workers with the same {@link
 * BigtableConfig} and read / write options.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableServiceFactory {

  static final BigtableServiceFactory FACTORY_INSTANCE = new BigtableServiceFactory();

  private int nextId = 0;

  private final Map<ConfigId, BigtableServiceEntry> readEntries = new HashMap<>();
  private final Map<ConfigId, BigtableServiceEntry> writeEntries = new HashMap<>();

  @AutoValue
  abstract static class ConfigId implements Serializable {

    abstract int id();

    static ConfigId create(int id) {
      return new AutoValue_BigtableServiceFactory_ConfigId(id);
    }
  }

  @AutoValue
  abstract static class BigtableServiceEntry {

    abstract ConfigId getConfigId();

    abstract BigtableService getService();

    abstract AtomicInteger getRefCount();

    // Workaround for ReadRows requests which requires to pass the timeouts in
    // ApiContext. Can be removed later once it's fixed in Veneer.
    abstract Duration getAttemptTimeout();

    abstract Duration getOperationTimeout();

    static BigtableServiceEntry create(
        ConfigId configId,
        BigtableService service,
        AtomicInteger refCount,
        Duration attemptTimeout,
        Duration operationTimeout) {
      return new AutoValue_BigtableServiceFactory_BigtableServiceEntry(
          configId, service, refCount, attemptTimeout, operationTimeout);
    }
  }

  synchronized BigtableServiceEntry getServiceForReading(
      ConfigId configId,
      BigtableConfig config,
      BigtableReadOptions opts,
      PipelineOptions pipelineOptions)
      throws IOException {
    BigtableServiceEntry entry = readEntries.get(configId);
    if (entry != null) {
      entry.getRefCount().incrementAndGet();
      return entry;
    }

    BigtableOptions effectiveOptions = config.getBigtableOptions();
    if (effectiveOptions == null && config.getBigtableOptionsConfigurator() != null) {
      effectiveOptions =
          config.getBigtableOptionsConfigurator().apply(BigtableOptions.builder()).build();
    }
    if (effectiveOptions != null) {
      // If BigtableOptions is set, convert it to BigtableConfig and BigtableWriteOptions
      config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
      opts = BigtableConfigTranslator.translateToBigtableReadOptions(opts, effectiveOptions);
    }
    BigtableDataSettings settings =
        BigtableConfigTranslator.translateReadToVeneerSettings(config, opts, pipelineOptions);
    BigtableService service = new BigtableServiceImpl(settings);
    RetrySettings retrySettings = settings.readRowSettings().getRetrySettings();
    entry =
        BigtableServiceEntry.create(
            configId,
            service,
            new AtomicInteger(1),
            retrySettings.getInitialRpcTimeout(),
            retrySettings.getTotalTimeout());
    readEntries.put(configId, entry);
    return entry;
  }

  synchronized BigtableServiceEntry getServiceForWriting(
      ConfigId configId,
      BigtableConfig config,
      BigtableWriteOptions opts,
      PipelineOptions pipelineOptions)
      throws IOException {
    BigtableServiceEntry entry = writeEntries.get(configId);
    if (entry != null) {
      entry.getRefCount().incrementAndGet();
      return entry;
    }

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateWriteToVeneerSettings(config, opts, pipelineOptions);
    BigtableService service = new BigtableServiceImpl(settings);
    RetrySettings retrySettings =
        settings.getStubSettings().bulkMutateRowsSettings().getRetrySettings();
    entry =
        BigtableServiceEntry.create(
            configId,
            service,
            new AtomicInteger(1),
            retrySettings.getInitialRpcTimeout(),
            retrySettings.getTotalTimeout());
    writeEntries.put(configId, entry);
    return entry;
  }

  synchronized void releaseReadService(BigtableServiceEntry entry) {
    if (entry.getRefCount().decrementAndGet() == 0) {
      //      entry.getService().close();
      readEntries.remove(entry.getConfigId());
    }
  }

  synchronized void releaseWriteService(BigtableServiceEntry entry) {
    if (entry.getRefCount().decrementAndGet() == 0) {
      //      entry.getService().close();
      writeEntries.remove(entry.getConfigId());
    }
  }

  synchronized ConfigId newId() {
    return ConfigId.create(nextId++);
  }

  @VisibleForTesting
  synchronized void addFakeWriteService(ConfigId id, BigtableService service) {
    writeEntries.put(
        id,
        BigtableServiceEntry.create(
            id, service, new AtomicInteger(1), Duration.ofMillis(100), Duration.ofMillis(1000)));
  }

  @VisibleForTesting
  synchronized void addFakeReadService(ConfigId id, BigtableService service) {
    readEntries.put(
        id,
        BigtableServiceEntry.create(
            id, service, new AtomicInteger(1), Duration.ofMillis(100), Duration.ofMillis(1000)));
  }
}
