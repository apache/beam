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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The {@link Table} returned by {@link BeamStreamingSource}. It only ever declares {@link
 * TableCapability#MICRO_BATCH_READ}, batch reads of an unbounded Beam source are handled elsewhere.
 */
public class BeamStreamingTable implements Table, SupportsRead {

  private final CaseInsensitiveStringMap options;

  BeamStreamingTable(CaseInsensitiveStringMap options) {
    this.options = options;
  }

  @Override
  public String name() {
    return "BeamUnboundedSource["
        + options.getOrDefault(BeamStreamingSource.OPT_SOURCE_ID, "?")
        + "]";
  }

  @Override
  public StructType schema() {
    return BeamStreamingSource.SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.MICRO_BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
    // Spark hands the full DataSourceV2 option map to both getTable and newScanBuilder. Prefer the
    // scan options and fall back to the table properties for anything missing.
    CaseInsensitiveStringMap merged = merge(options, scanOptions);
    return () -> new BeamScan(merged);
  }

  private static CaseInsensitiveStringMap merge(
      CaseInsensitiveStringMap base, CaseInsensitiveStringMap override) {
    java.util.Map<String, String> map = new java.util.HashMap<>(base.asCaseSensitiveMap());
    map.putAll(override.asCaseSensitiveMap());
    return new CaseInsensitiveStringMap(map);
  }

  /** The {@link Scan} of a Beam unbounded source, micro-batch only. */
  private static class BeamScan implements Scan {
    private final CaseInsensitiveStringMap options;

    BeamScan(CaseInsensitiveStringMap options) {
      this.options = options;
    }

    @Override
    public StructType readSchema() {
      return BeamStreamingSource.SCHEMA;
    }

    @Override
    public String description() {
      return "BeamUnboundedSource["
          + options.getOrDefault(BeamStreamingSource.OPT_SOURCE_ID, "?")
          + "]";
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
      return new BeamMicroBatchStream(options, checkpointLocation);
    }
  }
}
