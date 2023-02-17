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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that caches {@link BigtableService} to share between workers with the same {@link
 * BigtableConfig} and read / write options.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableServiceFactory implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableServiceFactory.class);

  static final BigtableServiceFactory FACTORY_INSTANCE = new BigtableServiceFactory();

  private transient int nextId = 0;

  private final transient Map<ConfigId, BigtableServiceEntry> readEntries = new HashMap<>();
  private final transient Map<ConfigId, BigtableServiceEntry> writeEntries = new HashMap<>();

  @AutoValue
  abstract static class ConfigId implements Serializable {

    abstract int id();

    static ConfigId create(int id) {
      return new AutoValue_BigtableServiceFactory_ConfigId(id);
    }
  }

  @AutoValue
  abstract static class BigtableServiceEntry implements Serializable, AutoCloseable {

    abstract BigtableServiceFactory getServiceFactory();

    abstract ConfigId getConfigId();

    abstract BigtableService getService();

    abstract AtomicInteger getRefCount();

    abstract String getServiceType();

    static BigtableServiceEntry create(
        BigtableServiceFactory factory,
        ConfigId configId,
        BigtableService service,
        AtomicInteger refCount,
        String serviceType) {
      return new AutoValue_BigtableServiceFactory_BigtableServiceEntry(
          factory, configId, service, refCount, serviceType);
    }

    @Override
    public void close() {
      if (getServiceType().equals("read")) {
        getServiceFactory().releaseReadService(this);
      } else if (getServiceType().equals("write")) {
        getServiceFactory().releaseWriteService(this);
      }
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

    BigtableOptions effectiveOptions = getEffectiveOptions(config);
    if (effectiveOptions != null) {
      // If BigtableOptions is set, convert it to BigtableConfig and BigtableWriteOptions
      config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
      opts = BigtableConfigTranslator.translateToBigtableReadOptions(opts, effectiveOptions);
    }
    BigtableDataSettings settings =
        BigtableConfigTranslator.translateReadToVeneerSettings(config, opts, pipelineOptions);
    BigtableService service = new BigtableServiceImpl(settings);
    entry = BigtableServiceEntry.create(this, configId, service, new AtomicInteger(1), "read");
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

    BigtableOptions effectiveOptions = getEffectiveOptions(config);
    if (effectiveOptions != null) {
      // If BigtableOptions is set, convert it to BigtableConfig and BigtableWriteOptions
      config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
      opts = BigtableConfigTranslator.translateToBigtableWriteOptions(opts, effectiveOptions);
    }

    BigtableDataSettings settings =
        BigtableConfigTranslator.translateWriteToVeneerSettings(config, opts, pipelineOptions);
    BigtableService service = new BigtableServiceImpl(settings);
    entry = BigtableServiceEntry.create(this, configId, service, new AtomicInteger(1), "write");
    writeEntries.put(configId, entry);
    return entry;
  }

  synchronized void releaseReadService(BigtableServiceEntry entry) {
    if (entry.getRefCount().decrementAndGet() == 0) {
      entry.getService().close();
      readEntries.remove(entry.getConfigId());
    }
  }

  synchronized void releaseWriteService(BigtableServiceEntry entry) {
    if (entry.getRefCount().decrementAndGet() == 0) {
      entry.getService().close();
      writeEntries.remove(entry.getConfigId());
    }
  }

  boolean checkTableExists(BigtableConfig config, PipelineOptions pipelineOptions, String tableId)
      throws IOException {
    BigtableOptions effectiveOptions = getEffectiveOptions(config);
    if (effectiveOptions != null) {
      config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
    }

    if (config.isDataAccessible()) {
      BigtableDataSettings settings =
          BigtableConfigTranslator.translateToVeneerSettings(config, pipelineOptions);

      try (BigtableDataClient client = BigtableDataClient.create(settings)) {
        try {
          client.readRow(tableId, "non-exist-row");
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            return false;
          }
          String message = String.format("Error checking whether table %s exists", tableId);
          LOG.error(message, e);
          throw new IOException(message, e);
        }
      }
    }
    return true;
  }

  synchronized ConfigId newId() {
    return ConfigId.create(nextId++);
  }

  private BigtableOptions getEffectiveOptions(BigtableConfig config) {
    BigtableOptions effectiveOptions = config.getBigtableOptions();
    if (effectiveOptions == null && config.getBigtableOptionsConfigurator() != null) {
      effectiveOptions =
          config.getBigtableOptionsConfigurator().apply(BigtableOptions.builder()).build();
    }
    return effectiveOptions;
  }
}
