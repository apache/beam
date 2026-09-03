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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;

/**
 * Collects the distinct schemas among canonical file schema JSONs (see {@link FileSchemas}), with
 * the number of files per schema, most common first (ties broken by JSON). The commit side applies
 * schemas in this order, so the schema covering the most files wins a conflict.
 *
 * <p>Inputs are compared as strings, so they must already be canonical.
 */
class CollectDistinctSchemas
    extends Combine.CombineFn<String, Map<String, Long>, List<KV<String, Long>>> {

  @Override
  public Map<String, Long> createAccumulator() {
    return new TreeMap<>();
  }

  @Override
  public Map<String, Long> addInput(Map<String, Long> accumulator, String schemaJson) {
    add(accumulator, schemaJson, 1L);
    return accumulator;
  }

  @Override
  public Map<String, Long> mergeAccumulators(Iterable<Map<String, Long>> accumulators) {
    Map<String, Long> merged = createAccumulator();
    for (Map<String, Long> accumulator : accumulators) {
      for (Map.Entry<String, Long> entry : accumulator.entrySet()) {
        add(merged, entry.getKey(), entry.getValue());
      }
    }
    return merged;
  }

  @Override
  public List<KV<String, Long>> extractOutput(Map<String, Long> accumulator) {
    List<KV<String, Long>> schemas = new ArrayList<>();
    for (Map.Entry<String, Long> entry : accumulator.entrySet()) {
      schemas.add(KV.of(entry.getKey(), entry.getValue()));
    }
    schemas.sort(
        (a, b) -> {
          int byCount = Long.compare(b.getValue(), a.getValue());
          if (byCount != 0) {
            return byCount;
          }
          return a.getKey().compareTo(b.getKey());
        });
    return schemas;
  }

  @Override
  public Coder<Map<String, Long>> getAccumulatorCoder(
      CoderRegistry registry, Coder<String> inputCoder) {
    return MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
  }

  @Override
  public Coder<List<KV<String, Long>>> getDefaultOutputCoder(
      CoderRegistry registry, Coder<String> inputCoder) {
    return ListCoder.of(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
  }

  private static void add(Map<String, Long> accumulator, String schemaJson, long count) {
    Long existing = accumulator.get(schemaJson);
    if (existing == null) {
      accumulator.put(schemaJson, count);
    } else {
      accumulator.put(schemaJson, existing + count);
    }
  }
}
