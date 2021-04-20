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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Charsets;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

public class ResourceHints {
  // TODO: reference a const from compiled proto.
  private static final String MEMORY_URN = "beam:resources:min_ram_bytes:v1";
  private static final String ACCELERATOR_URN = "beam:resources:accelerator:v1";

  private static ImmutableMap<String, String> ABBREVIATIONS =
      ImmutableMap.<String, String>builder()
          .put("minRam", MEMORY_URN)
          .put("accelerator", ACCELERATOR_URN)
          .build();

  private static ImmutableMap<String, Function<String, ResourceHint>> PARSERS =
      ImmutableMap.<String, Function<String, ResourceHint>>builder()
          .put(MEMORY_URN, s -> new BytesHint(BytesHint.parse(s)))
          .put(ACCELERATOR_URN, s -> new StringHint(s))
          .build();

  private static ResourceHints EMPTY = new ResourceHints(ImmutableMap.of());

  private final ImmutableMap<String, ResourceHint> hints;

  private ResourceHints(ImmutableMap<String, ResourceHint> hints) {
    this.hints = hints;
  }

  public static ResourceHints create() {
    return EMPTY;
  }

  public static ResourceHints fromOptions(PipelineOptions options) {
    ResourceHintsOptions resourceHintsOptions = options.as(ResourceHintsOptions.class);
    ResourceHints result = create();
    if (resourceHintsOptions.getResourceHints() == null) {
      return result;
    }
    Splitter splitter = Splitter.on('=').limit(2);
    for (String hint : resourceHintsOptions.getResourceHints()) {
      List<String> parts = splitter.splitToList(hint);
      if (parts.size() != 2) {
        throw new IllegalArgumentException("Unparsable resource hint: " + hint);
      }
      String urn = parts.get(0);
      String stringValue = parts.get(1);
      if (ABBREVIATIONS.containsKey(urn)) {
        urn = ABBREVIATIONS.get(urn);
      } else if (!urn.startsWith("beam:resources:")) {
        // Allow unknown hints to be passed, but validate a little bit to prevent typos.
        throw new IllegalArgumentException("Unknown resource hint: " + hint);
      }
      ResourceHint value = PARSERS.getOrDefault(urn, s -> new StringHint(s)).apply(stringValue);
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
    public boolean equals(Object other) {
      if (this == other) {
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
      Matcher m = Pattern.compile("([\\d.]+)[\\s]?(([KMGTP]i?)?B)").matcher(s);
      // Matcher m = Pattern.compile("^\\d\\.?\\d*").matcher(s);
      if (m.find()) {
        String number = m.group(1);
        String suffix = m.group(2);
        if (suffixes.containsKey(suffix)) {
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
      return String.valueOf(value).getBytes(Charsets.US_ASCII);
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
    public ResourceHint mergeWithOuter(ResourceHint outer) {
      return this;
    }

    @Override
    public byte[] toBytes() {
      return value.getBytes(Charsets.US_ASCII);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
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

  public ResourceHints withMemory(long ramBytes) {
    return withHint(MEMORY_URN, new BytesHint(ramBytes));
  }

  public ResourceHints withMemory(String ramBytes) {
    return withMemory(BytesHint.parse(ramBytes));
  }

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

  public ResourceHints withAccelerator(String accelerator) {
    return withHint(ACCELERATOR_URN, new StringHint(accelerator));
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
      for (Map.Entry hint : hints.entrySet()) {
        if (!outer.hints.containsKey(hint.getKey())) {
          newHints.put(hint);
        }
      }
      return new ResourceHints(newHints.build());
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
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
