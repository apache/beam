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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

public class ResourceHints {
  private static final String MEMORY_URN = "TODO: memory";

  private static ResourceHints EMPTY = new ResourceHints(ImmutableMap.of());

  private final ImmutableMap<String, ResourceHint> hints;

  private ResourceHints(ImmutableMap<String, ResourceHint> hints) {
    this.hints = hints;
  }

  public static ResourceHints create() {
    return EMPTY;
  }

  public static ResourceHints fromOptions(PipelineOptions options) {
    // TODO: Add pipeline options.
    return create();
  }

  private static class BytesHint implements ResourceHint {
    private static Map<String, Long> suffixes =
        ImmutableMap.<String, Long>builder()
            .put("b", 1L)
            .put("kb", 1000L)
            .put("mb", 1000_000L)
            .put("gb", 1000_000_000L)
            .put("tb", 1000_000_000_000L)
            .put("pb", 1000_000_000_000_000L)
            .put("kib", 1L << 10)
            .put("mib", 1L << 20)
            .put("gib", 1L << 30)
            .put("tib", 1L << 40)
            .put("pib", 1L << 50)
            .build();

    private final long value;

    public BytesHint(long value) {
      this.value = value;
    }

    public static long parse(String s) {
      Matcher m = Pattern.compile("^\\d_\\.?\\d*").matcher(s);
      if (m.find()) {
        String number = s.substring(0, m.end());
        String suffix = s.substring(m.end());
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
