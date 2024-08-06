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
package org.apache.beam.sdk.transforms.resourcehints;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardResourceHints;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ProtocolMessageEnum;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline authors can use resource hints to provide additional information to runners about the
 * desired aspects of the execution environment. Resource hints can be specified via {@link
 * org.apache.beam.sdk.transforms.PTransform PTransform#setResourceHints} for parts of the pipeline,
 * or globally via {@link ResourceHintsOptions resourceHints} pipeline option.
 *
 * <p>Interpretation of hints is provided by Beam runners.
 */
public class ResourceHints {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceHints.class);
  private static final String MIN_RAM_URN = "beam:resources:min_ram_bytes:v1";
  private static final String ACCELERATOR_URN = "beam:resources:accelerator:v1";

  private static final String CPU_COUNT_URN = "beam:resources:cpu_count:v1";

  // TODO: reference this from a common location in all packages that use this.
  private static String getUrn(ProtocolMessageEnum value) {
    return value.getValueDescriptor().getOptions().getExtension(RunnerApi.beamUrn);
  }

  static {
    checkState(MIN_RAM_URN.equals(getUrn(StandardResourceHints.Enum.MIN_RAM_BYTES)));
    checkState(ACCELERATOR_URN.equals(getUrn(StandardResourceHints.Enum.ACCELERATOR)));
    checkState(CPU_COUNT_URN.equals(getUrn(StandardResourceHints.Enum.CPU_COUNT)));
  }

  private static ImmutableMap<String, String> hintNameToUrn =
      ImmutableMap.<String, String>builder()
          .put("minRam", MIN_RAM_URN)
          .put("min_ram", MIN_RAM_URN) // Courtesy alias.
          .put("accelerator", ACCELERATOR_URN)
          .put("cpuCount", CPU_COUNT_URN)
          .put("cpu_count", CPU_COUNT_URN) // Courtesy alias.
          .build();

  private static ImmutableMap<String, Function<String, ResourceHint>> parsers =
      ImmutableMap.<String, Function<String, ResourceHint>>builder()
          .put(MIN_RAM_URN, s -> new BytesHint(BytesHint.parse(s)))
          .put(ACCELERATOR_URN, s -> new StringHint(s))
          .put(CPU_COUNT_URN, s -> new IntHint(IntHint.parse(s)))
          .build();

  private static final ResourceHints EMPTY = new ResourceHints(ImmutableMap.of());

  private final ImmutableMap<String, ResourceHint> hints;

  private ResourceHints(ImmutableMap<String, ResourceHint> hints) {
    this.hints = hints;
  }

  /** Creates a {@link ResourceHints} instance with no hints. */
  public static ResourceHints create() {
    return EMPTY;
  }

  /** Creates a {@link ResourceHints} instance with hints supplied in options. */
  public static ResourceHints fromOptions(PipelineOptions options) {
    ResourceHintsOptions resourceHintsOptions = options.as(ResourceHintsOptions.class);
    ResourceHints result = create();
    List<String> hints = resourceHintsOptions.getResourceHints();
    Splitter splitter = Splitter.on('=').limit(2);
    for (String hint : hints) {
      List<String> parts = splitter.splitToList(hint);
      if (parts.size() != 2) {
        throw new IllegalArgumentException("Unparsable resource hint: " + hint);
      }
      String nameOrUrn = parts.get(0);
      String stringValue = parts.get(1);
      String urn;
      if (hintNameToUrn.containsKey(nameOrUrn)) {
        urn = hintNameToUrn.get(nameOrUrn);
      } else if (!nameOrUrn.startsWith("beam:resources:")) {
        // Allow unknown hints to be passed, but validate a little bit to prevent typos.
        throw new IllegalArgumentException("Unknown resource hint: " + hint);
      } else {
        urn = nameOrUrn;
      }
      ResourceHint value =
          Preconditions.checkNotNull(parsers.getOrDefault(urn, StringHint::new)).apply(stringValue);
      result = result.withHint(urn, value);
    }
    return result;
  }

  /*package*/ static class BytesHint extends ResourceHint {
    private static Map<String, Long> suffixes =
        ImmutableMap.<String, Long>builder()
            .put("B", 1L)
            .put("KB", 1000L)
            .put("MB", 1000_000L)
            .put("GB", 1000_000_000L)
            .put("TB", 1000_000_000_000L)
            .put("PB", 1000_000_000_000_000L)
            .put("KiB", 1L << 10)
            .put("MiB", 1L << 20)
            .put("GiB", 1L << 30)
            .put("TiB", 1L << 40)
            .put("PiB", 1L << 50)
            .build();

    private final long value;

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null) {
        return false;
      } else if (this == other) {
        return true;
      } else if (other instanceof BytesHint) {
        return ((BytesHint) other).value == value;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    public BytesHint(long value) {
      this.value = value;
    }

    public static long parse(String s) {
      Matcher m = Pattern.compile("([\\d.]+)[\\s]?([\\D]+$)").matcher(s);
      if (m.find()) {
        String number = m.group(1);
        String suffix = m.group(2);
        if (number != null && suffix != null && suffixes.containsKey(suffix)) {
          return (long) (Double.valueOf(number) * suffixes.get(suffix));
        }
      }
      throw new IllegalArgumentException("Unable to parse '" + s + "' as a byte value.");
    }

    @Override
    public ResourceHint mergeWithOuter(ResourceHint outer) {
      return new BytesHint(Math.max(value, ((BytesHint) outer).value));
    }

    @Override
    public byte[] toBytes() {
      return String.valueOf(value).getBytes(StandardCharsets.US_ASCII);
    }
  }

  /*package*/ static class StringHint extends ResourceHint {
    private final String value;

    public StringHint(String value) {
      this.value = value;
    }

    public static String parse(String s) {
      return s;
    }

    @Override
    public byte[] toBytes() {
      return value.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null) {
        return false;
      } else if (this == other) {
        return true;
      } else if (other instanceof StringHint) {
        return ((StringHint) other).value.equals(value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  /*package*/ static class IntHint extends ResourceHint {
    private final int value;

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null) {
        return false;
      } else if (this == other) {
        return true;
      } else if (other instanceof IntHint) {
        return ((IntHint) other).value == value;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(value);
    }

    public IntHint(int value) {
      this.value = value;
    }

    public static int parse(String s) {
      return Integer.parseInt(s, 10);
    }

    @Override
    public ResourceHint mergeWithOuter(ResourceHint outer) {
      return new IntHint(Math.max(value, ((IntHint) outer).value));
    }

    @Override
    public byte[] toBytes() {
      return String.valueOf(value).getBytes(StandardCharsets.US_ASCII);
    }
  }

  /**
   * Sets desired minimal available RAM size to have in transform's execution environment.
   *
   * @param ramBytes specifies a positive RAM size in bytes. A number greater than 2G
   *     (Integer.MAX_VALUE) is typical.
   */
  public ResourceHints withMinRam(long ramBytes) {
    if (ramBytes <= 0L) {
      // TODO(yathu) ignore invalid value as of Beam v2.47.0. throw error in future version.
      LOG.error(
          "Encountered invalid non-positive minimum ram hint value {}.\n"
              + "Likely cause is an (overflowing) int expression is passed in. "
              + "The value is ignored. In the future, The method will require an object Long type "
              + "and throw an IllegalArgumentException for invalid values.",
          ramBytes);
      return this;
    } else if (ramBytes <= Integer.MAX_VALUE) {
      LOG.warn(
          "Minimum available RAM size ({}) is set too small.\n"
              + "Likely cause is an (overflowing) int expression is passed in.",
          ramBytes);
    }
    return withHint(MIN_RAM_URN, new BytesHint(ramBytes));
  }

  /**
   * Sets desired minimal available RAM size to have in transform's execution environment.
   *
   * @param ramBytes specifies a human-friendly size string, for example: '10.5 GiB', '4096 MiB',
   *     etc.
   */
  public ResourceHints withMinRam(String ramBytes) {
    return withMinRam(BytesHint.parse(ramBytes));
  }

  /** Declares hardware accelerators that are desired to have in the execution environment. */
  public ResourceHints withAccelerator(String accelerator) {
    return withHint(ACCELERATOR_URN, new StringHint(accelerator));
  }

  /** Declares a custom resource hint that has a specified URN. */
  public ResourceHints withHint(String urn, ResourceHint hint) {
    ImmutableMap.Builder<String, ResourceHint> newHints = ImmutableMap.builder();
    newHints.put(urn, hint);
    for (Map.Entry<String, ResourceHint> oldHint : hints.entrySet()) {
      if (!oldHint.getKey().equals(urn)) {
        newHints.put(oldHint.getKey(), oldHint.getValue());
      }
    }
    return new ResourceHints(newHints.build());
  }

  /**
   * Sets desired minimal CPU or vCPU count to have in transform's execution environment.
   *
   * @param cpuCount specifies a positive CPU count.
   */
  public ResourceHints withCPUCount(int cpuCount) {
    if (cpuCount <= 0) {
      LOG.error(
          "Encountered invalid non-positive cpu count hint value {}.\n"
              + "The value is ignored. In the future, The method will require an object Long type "
              + "and throw an IllegalArgumentException for invalid values.",
          cpuCount);
      return this;
    }
    return withHint(CPU_COUNT_URN, new IntHint(cpuCount));
  }

  public Map<String, ResourceHint> hints() {
    return hints;
  }

  public ResourceHints mergeWithOuter(ResourceHints outer) {
    if (outer.hints.isEmpty()) {
      return this;
    } else if (hints.isEmpty()) {
      return outer;
    } else {
      ImmutableMap.Builder<String, ResourceHint> newHints = ImmutableMap.builder();
      for (Map.Entry<String, ResourceHint> outerHint : outer.hints().entrySet()) {
        if (hints.containsKey(outerHint.getKey())) {
          newHints.put(
              outerHint.getKey(),
              hints.get(outerHint.getKey()).mergeWithOuter(outerHint.getValue()));
        } else {
          newHints.put(outerHint);
        }
      }
      for (Map.Entry<String, ResourceHint> hint : hints.entrySet()) {
        if (!outer.hints.containsKey(hint.getKey())) {
          newHints.put(hint);
        }
      }
      return new ResourceHints(newHints.build());
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == null) {
      return false;
    } else if (this == other) {
      return true;
    } else if (other instanceof ResourceHints) {
      return ((ResourceHints) other).hints.equals(hints);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return hints.hashCode();
  }
}
