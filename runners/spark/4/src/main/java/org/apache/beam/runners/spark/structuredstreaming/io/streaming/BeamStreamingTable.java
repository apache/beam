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

/** DataSourceV2 {@link Table} over a Beam unbounded source, micro-batch reads only. */
public class BeamStreamingTable implements Table, SupportsRead {

  private final BeamSourceSpec<?> spec;

  BeamStreamingTable(BeamSourceSpec<?> spec) {
    this.spec = spec;
  }

  @Override
  public String name() {
    return "BeamUnboundedSource[" + spec.transformName() + "]";
  }

  @Override
  public StructType schema() {
    return UnboundedSourceDataset.SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.MICRO_BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap ignored) {
    return () -> new BeamScan(spec);
  }

  private static class BeamScan implements Scan {
    private final BeamSourceSpec<?> spec;

    BeamScan(BeamSourceSpec<?> spec) {
      this.spec = spec;
    }

    @Override
    public StructType readSchema() {
      return UnboundedSourceDataset.SCHEMA;
    }

    @Override
    public String description() {
      return "BeamUnboundedSource[" + spec.transformName() + "]";
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
      return new BeamMicroBatchStream<>(spec, checkpointLocation);
    }
  }
}
