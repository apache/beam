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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableServiceFactory.BigtableServiceEntry.CloseMode;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that caches {@link BigtableService} to share between workers with the same {@link
 * BigtableConfig} and read / write options. A new {@link ConfigId} is created at graph construction
 * time, and each {@link BigtableService} is mapped to one {@link ConfigId}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableServiceFactory implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableServiceFactory.class);

  private static final ConcurrentHashMap<UUID, BigtableServiceEntry> entries =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<UUID, AtomicInteger> refCounts = new ConcurrentHashMap<>();
  private static final Object lock = new Object();

  private static final SelfClosingExecutor ENTRY_CLOSER = new SelfClosingExecutor();

  private static final String BIGTABLE_ENABLE_CLIENT_SIDE_METRICS =
      "bigtable_enable_client_side_metrics";

  private static final String BIGTABLE_READ_CLOSE_MODE = "bigtable_read_close_mode";
  private static final String BIGTABLE_READ_CLOSE_DELAY_SECS = "bigtable_read_close_delay_secs";

  @AutoValue
  abstract static class ConfigId implements Serializable {

    abstract UUID id();

    static ConfigId create() {
      return new AutoValue_BigtableServiceFactory_ConfigId(UUID.randomUUID());
    }
  }

  @AutoValue
  abstract static class BigtableServiceEntry implements Serializable, AutoCloseable {
    enum CloseMode {
      NONE,
      DELAYED,
      INLINE
    }

    abstract ConfigId getConfigId();

    abstract BigtableService getService();

    abstract CloseMode getCloseMode();

    @Nullable
    abstract Duration getCloseDelay();

    static BigtableServiceEntry create(ConfigId configId, BigtableService service) {
      return new AutoValue_BigtableServiceFactory_BigtableServiceEntry(
          configId, service, CloseMode.INLINE);
    }

    static BigtableServiceEntry create(
        ConfigId configId, BigtableService service, CloseMode closeMode, Duration closeDelay) {
      return new AutoValue_BigtableServiceFactory_BigtableServiceEntry(
          configId, service, closeMode, closeDelay);
    }

    @Override
    public void close() {
      switch (getCloseMode()) {
        case NONE:
          return;
        case INLINE:
          closeImpl();
          return;
        case DELAYED:
          ENTRY_CLOSER.enqueue(this, getCloseDelay());
      }
    }

    private void closeImpl() {
      synchronized (lock) {
        int refCount =
            refCounts.getOrDefault(getConfigId().id(), new AtomicInteger(0)).decrementAndGet();
        if (refCount < 0) {
          LOG.error(
              "close() Ref count is < 0, configId=" + getConfigId().id() + " refCount=" + refCount);
        }
        LOG.debug(
            "close() is called for config id " + getConfigId().id() + ", ref count is " + refCount);
        if (refCount == 0) {
          entries.remove(getConfigId().id());
          refCounts.remove(getConfigId().id());
          getService().close();
        }
      }
    }
  }

  BigtableServiceEntry getServiceForReading(
      ConfigId configId,
      BigtableConfig config,
      BigtableReadOptions opts,
      PipelineOptions pipelineOptions)
      throws IOException {
    synchronized (lock) {
      LOG.debug("getServiceForReading(), config id: " + configId.id());
      BigtableServiceEntry entry = entries.get(configId.id());
      if (entry != null) {
        // When entry is not null, refCount.get(configId.id()) should always exist.
        // Doing a putIfAbsent to avoid NPE.
        AtomicInteger count = refCounts.putIfAbsent(configId.id(), new AtomicInteger(0));
        if (count == null) {
          LOG.error("entry is not null but refCount of config Id " + configId.id() + " is null.");
        }
        refCounts.get(configId.id()).getAndIncrement();
        LOG.debug("getServiceForReading() returning an existing service entry");
        return entry;
      }

      BigtableOptions effectiveOptions = getEffectiveOptions(config);
      BigtableReadOptions optsFromBigtableOptions = null;
      if (effectiveOptions != null) {
        // If BigtableOptions is set, convert it to BigtableConfig and BigtableReadOptions
        config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
        optsFromBigtableOptions =
            BigtableConfigTranslator.translateToBigtableReadOptions(opts, effectiveOptions);
      }
      BigtableDataSettings settings =
          BigtableConfigTranslator.translateReadToVeneerSettings(
              config, opts, optsFromBigtableOptions, pipelineOptions);

      if (ExperimentalOptions.hasExperiment(pipelineOptions, BIGTABLE_ENABLE_CLIENT_SIDE_METRICS)) {
        LOG.info("Enabling client side metrics");
        BigtableDataSettings.enableBuiltinMetrics();
      }
      CloseMode closeMode =
          Optional.ofNullable(
                  ExperimentalOptions.getExperimentValue(pipelineOptions, BIGTABLE_READ_CLOSE_MODE))
              .map(BigtableServiceEntry.CloseMode::valueOf)
              .orElse(CloseMode.DELAYED);

      Duration closeDelaySecs =
          Optional.ofNullable(
                  ExperimentalOptions.getExperimentValue(
                      pipelineOptions, BIGTABLE_READ_CLOSE_DELAY_SECS))
              .map(Integer::parseInt)
              .map(Duration::ofSeconds)
              .orElse(Duration.ofSeconds(10));

      BigtableService service = new BigtableServiceImpl(settings);
      entry = BigtableServiceEntry.create(configId, service, closeMode, closeDelaySecs);
      entries.put(configId.id(), entry);
      refCounts.put(configId.id(), new AtomicInteger(1));
      LOG.debug("getServiceForReading() created a new service entry");
      return entry;
    }
  }

  BigtableServiceEntry getServiceForWriting(
      ConfigId configId,
      BigtableConfig config,
      BigtableWriteOptions opts,
      PipelineOptions pipelineOptions)
      throws IOException {
    synchronized (lock) {
      BigtableServiceEntry entry = entries.get(configId.id());
      LOG.debug("getServiceForWriting(), config id: " + configId.id());
      if (entry != null) {
        // When entry is not null, refCount.get(configId.id()) should always exist.
        // Doing a putIfAbsent to avoid NPE.
        AtomicInteger count = refCounts.putIfAbsent(configId.id(), new AtomicInteger(0));
        if (count == null) {
          LOG.error("entry is not null but refCount of config Id " + configId.id() + " is null.");
        }
        refCounts.get(configId.id()).getAndIncrement();
        LOG.debug("getServiceForWriting() returning an existing service entry");
        return entry;
      }

      BigtableOptions effectiveOptions = getEffectiveOptions(config);
      BigtableWriteOptions optsFromBigtableOptions = null;
      if (effectiveOptions != null) {
        // If BigtableOptions is set, convert it to BigtableConfig and BigtableWriteOptions
        config = BigtableConfigTranslator.translateToBigtableConfig(config, effectiveOptions);
        optsFromBigtableOptions =
            BigtableConfigTranslator.translateToBigtableWriteOptions(opts, effectiveOptions);
      }

      BigtableDataSettings settings =
          BigtableConfigTranslator.translateWriteToVeneerSettings(
              config, opts, optsFromBigtableOptions, pipelineOptions);

      if (ExperimentalOptions.hasExperiment(pipelineOptions, BIGTABLE_ENABLE_CLIENT_SIDE_METRICS)) {
        LOG.info("Enabling client side metrics");
        BigtableDataSettings.enableBuiltinMetrics();
      }

      BigtableService service = new BigtableServiceImpl(settings);
      entry = BigtableServiceEntry.create(configId, service);
      entries.put(configId.id(), entry);
      refCounts.put(configId.id(), new AtomicInteger(1));
      LOG.debug("getServiceForWriting() created a new service entry");
      return entry;
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
        } catch (ApiException e) {
          if (e.getStatusCode().getCode() == GrpcStatusCode.Code.NOT_FOUND) {
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

  @VisibleForTesting
  static boolean isEmpty() {
    synchronized (lock) {
      return entries.isEmpty();
    }
  }

  synchronized ConfigId newId() {
    return ConfigId.create();
  }

  private BigtableOptions getEffectiveOptions(BigtableConfig config) {
    BigtableOptions effectiveOptions = config.getBigtableOptions();
    if (effectiveOptions == null && config.getBigtableOptionsConfigurator() != null) {
      effectiveOptions =
          config.getBigtableOptionsConfigurator().apply(BigtableOptions.builder()).build();
    }
    return effectiveOptions;
  }

  // TODO: Remove this after migrating to an SDF for BigtableIO.read()
  // This is only necessary because the Source api does not provide a hook into worker teardown
  // event. This workaround will extend the lifetime of each service entry just long enough so that
  // the refcount does not reach 0.
  /**
   * Simple wrapper around ScheduledThreadPoolExecutor that auto closes itself. It's meant to have a
   * similar behavior as ScheduledThreadPoolExecutor with 0 core threads, which is unfortunately
   * broken in jdk < 9.
   */
  static class SelfClosingExecutor {
    private ScheduledExecutorService executor = null;
    private int numOutstanding = 0;

    synchronized void enqueue(BigtableServiceEntry entry, Duration closeDelay) {
      if (numOutstanding == 0) {
        executor = Executors.newScheduledThreadPool(1);
      }
      numOutstanding++;

      executor.schedule(
          () -> {
            try {
              entry.close();
            } finally {
              synchronized (SelfClosingExecutor.this) {
                if (--numOutstanding == 0) {
                  executor.shutdown();
                  executor = null;
                }
                ;
              }
            }
          },
          closeDelay.getSeconds(),
          TimeUnit.SECONDS);
    }
  }
}
