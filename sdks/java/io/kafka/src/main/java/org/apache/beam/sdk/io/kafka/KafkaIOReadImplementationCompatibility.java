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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementation.LEGACY;
import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementation.SDF;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSortedSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link KafkaIO.Read} has multiple implementations with different feature set.<br>
 * This class is responsible to determine which one could/should be used.<br>
 * It is required because the methods used to configure the expected behaviour are shared,<br>
 * and not every configuration is being supported by every implementation.
 */
// ·TODO(https://github.com/apache/beam/issues/21482)·2022-04-15:·Remove·compatibility·testing·related·code·from·KafkaIO.Read·after
// ·SDF·implementation·has·fully·replaced·the·legacy·read
class KafkaIOReadImplementationCompatibility {

  /**
   * This enum should represent every different implementation<br>
   * that might be applied to the pipeline inside {@link KafkaIO.Read#expand(PBegin)}.
   */
  enum KafkaIOReadImplementation {
    /**
     * This is essentially a "traditional"/"legacy" read implementation,<br>
     * which uses {@link org.apache.beam.sdk.io.Read.Unbounded}<br>
     * to read from an {@link org.apache.beam.sdk.io.UnboundedSource}.
     *
     * @see KafkaIO.Read.ReadFromKafkaViaUnbounded
     * @see KafkaUnboundedSource
     * @see KafkaUnboundedReader
     */
    LEGACY,
    /**
     * This is the newer SplittableDoFn based implementation.
     *
     * @see KafkaIO.Read.ReadFromKafkaViaSDF
     * @see KafkaIO.ReadSourceDescriptors
     * @see ReadFromKafkaDoFn
     */
    SDF
  }

  /** This enum should represent every configurable property found at {@link KafkaIO.Read}. */
  @VisibleForTesting
  // errorprone doesn't recognize vendored guava immutable collection as immutable:
  // https://github.com/google/error-prone/blob/bc3309a7dbe95d006ee190fb36f2d654779858d4/core/src/main/java/com/google/errorprone/bugpatterns/ImmutableCollections.java#L75
  @SuppressWarnings("ImmutableEnumChecker")
  enum KafkaIOReadProperties {
    CONSUMER_CONFIG,
    TOPICS,
    TOPIC_PARTITIONS,
    TOPIC_PATTERN,
    KEY_CODER,
    VALUE_CODER,
    CONSUMER_FACTORY_FN,
    WATERMARK_FN(LEGACY),
    MAX_NUM_RECORDS(LEGACY) {
      @Override
      Object getDefaultValue() {
        return Long.MAX_VALUE;
      }
    },
    MAX_READ_TIME(LEGACY),
    START_READ_TIME,
    STOP_READ_TIME(SDF),
    COMMIT_OFFSETS_IN_FINALIZE_ENABLED {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    DYNAMIC_READ(SDF) {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    WATCH_TOPIC_PARTITION_DURATION(SDF),
    TIMESTAMP_POLICY_FACTORY,
    OFFSET_CONSUMER_CONFIG,
    KEY_DESERIALIZER_PROVIDER,
    VALUE_DESERIALIZER_PROVIDER,
    CHECK_STOP_READING_FN(SDF),
    BAD_RECORD_ERROR_HANDLER(SDF),
    CONSUMER_POLLING_TIMEOUT(SDF) {
      @Override
      Object getDefaultValue() {
        return Long.valueOf(2);
      }
    },
    REDISTRIBUTE_NUM_KEYS {
      @Override
      Object getDefaultValue() {
        return Integer.valueOf(0);
      }
    },
    REDISTRIBUTED {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    ALLOW_DUPLICATES {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    ;

    private final @NonNull ImmutableSet<KafkaIOReadImplementation> supportedImplementations;

    private KafkaIOReadProperties() {
      this(KafkaIOReadImplementation.values());
    }

    private KafkaIOReadProperties(@NonNull KafkaIOReadImplementation... supportedImplementations) {
      this.supportedImplementations =
          Sets.immutableEnumSet(Arrays.asList(supportedImplementations));
    }

    @VisibleForTesting
    static Method findGetterMethod(KafkaIOReadProperties property) {
      final String propertyNameInUpperCamel =
          CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, property.name());
      try {
        return KafkaIO.Read.class.getDeclaredMethod("get" + propertyNameInUpperCamel);
      } catch (NoSuchMethodException e) {
        try {
          return KafkaIO.Read.class.getDeclaredMethod("is" + propertyNameInUpperCamel);
        } catch (NoSuchMethodException e2) {
          throw new RuntimeException("Should not happen", e);
        }
      }
    }

    /**
     * In order to determine which property has been configured<br>
     * we compare the current value with the not configured one.<br>
     * Usually that is {@code null}, but in some cases it isn't.<br>
     * This method can be used to provide that value.
     */
    @VisibleForTesting
    @Nullable
    Object getDefaultValue() {
      return null;
    }
  }

  /**
   * Calculates what kind of read implementation can be used by the {@link KafkaIO.Read}.<br>
   * It enforces to "use" every configuration by picking an implementation that will do that.<br>
   * Fails if it's not doable due conflicting needs.<br>
   */
  static KafkaIOReadImplementationCompatibilityResult getCompatibility(KafkaIO.Read<?, ?> read) {
    final Multimap<KafkaIOReadImplementation, KafkaIOReadProperties>
        notSupportedImplementationsWithProperties = HashMultimap.create();
    for (KafkaIOReadProperties property : KafkaIOReadProperties.values()) {
      final EnumSet<KafkaIOReadImplementation> notSupportedImplementations =
          EnumSet.complementOf(EnumSet.copyOf(property.supportedImplementations));
      if (notSupportedImplementations.isEmpty()) {
        // if we support every implementation we can skip this check
        continue;
      }
      final Object defaultValue = property.getDefaultValue();
      final Object currentValue;
      try {
        currentValue = KafkaIOReadProperties.findGetterMethod(property).invoke(read);
      } catch (Exception e) {
        throw new RuntimeException("Should not happen", e);
      }
      if (Objects.equals(defaultValue, currentValue)) {
        // the defaultValue is always allowed,
        // so there would be no compatibility issue
        continue;
      }
      // the property has got a value, so we can't allow the not-supported implementations
      for (KafkaIOReadImplementation notSupportedImplementation : notSupportedImplementations) {
        notSupportedImplementationsWithProperties.put(notSupportedImplementation, property);
      }
    }
    if (EnumSet.allOf(KafkaIOReadImplementation.class)
        .equals(notSupportedImplementationsWithProperties.keySet())) {
      throw new IllegalStateException(
          "There is no Kafka read implementation that supports every configured property! "
              + "Not supported implementations with the associated properties: "
              + notSupportedImplementationsWithProperties);
    }
    return new KafkaIOReadImplementationCompatibilityResult(
        notSupportedImplementationsWithProperties);
  }

  static class KafkaIOReadImplementationCompatibilityResult {
    private final Multimap<KafkaIOReadImplementation, KafkaIOReadProperties> notSupported;

    private KafkaIOReadImplementationCompatibilityResult(
        Multimap<KafkaIOReadImplementation, KafkaIOReadProperties>
            notSupportedImplementationsWithAssociatedProperties) {
      this.notSupported = notSupportedImplementationsWithAssociatedProperties;
    }

    /**
     * @return true, if the implementation can "use" every configuration,<br>
     *     false, otherwise
     */
    boolean supports(KafkaIOReadImplementation implementation) {
      return !notSupported.containsKey(implementation);
    }

    /**
     * @return true, if the implementation - and only that - can "use" every configuration, <br>
     *     false, otherwise
     */
    boolean supportsOnly(KafkaIOReadImplementation implementation) {
      return EnumSet.complementOf(EnumSet.of(implementation)).equals(notSupported.keySet());
    }

    /**
     * Checks if the selected implementation can "use" every configuration,<br>
     * fails otherwise.
     */
    void checkSupport(KafkaIOReadImplementation selectedImplementation) {
      if (!supports(selectedImplementation)) {
        throw new KafkaIOReadImplementationCompatibilityException(
            selectedImplementation, notSupported.get(selectedImplementation));
      }
    }
  }

  static class KafkaIOReadImplementationCompatibilityException extends IllegalStateException {

    private final ImmutableSortedSet<KafkaIOReadProperties> conflictingProperties;

    private KafkaIOReadImplementationCompatibilityException(
        KafkaIOReadImplementation selectedImplementation,
        Collection<KafkaIOReadProperties> conflictingProperties) {
      super(
          "The current Kafka read configuration isn't supported by the "
              + selectedImplementation
              + " read implementation! "
              + "Conflicting properties: "
              + conflictingProperties);
      this.conflictingProperties = ImmutableSortedSet.copyOf(conflictingProperties);
    }

    ImmutableSortedSet<KafkaIOReadProperties> getConflictingProperties() {
      return conflictingProperties;
    }
  }
}
