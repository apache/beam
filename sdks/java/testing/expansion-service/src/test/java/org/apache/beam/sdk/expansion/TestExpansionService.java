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
package org.apache.beam.sdk.expansion;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * An {@link org.apache.beam.runners.core.construction.expansion.ExpansionService} useful for tests.
 */
public class TestExpansionService {

  private static final String TEST_COUNT_URN = "beam:transforms:xlang:count";
  private static final String TEST_FILTER_URN = "beam:transforms:xlang:filter_less_than_eq";
  private static final String TEST_PARQUET_READ_URN = "beam:transforms:xlang:parquet_read";
  private static final String TEST_PARQUET_WRITE_URN = "beam:transforms:xlang:parquet_write";

  /** Registers a single test transformation. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    String rawSchema =
        "{ \"type\": \"record\", \"name\": \"testrecord\", \"fields\": "
            + "[ {\"name\": \"name\", \"type\": \"string\"} ]}";

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      Schema schema = new Schema.Parser().parse(rawSchema);
      return ImmutableMap.of(
          TEST_COUNT_URN, spec -> Count.perElement(),
          TEST_FILTER_URN, spec -> Filter.lessThanEq(spec.getPayload().toStringUtf8()),
          TEST_PARQUET_READ_URN,
              spec ->
                  new PTransform<PBegin, PCollection<GenericRecord>>() {
                    @Override
                    public PCollection<GenericRecord> expand(PBegin input) {
                      return input
                          .apply(FileIO.match().filepattern(spec.getPayload().toStringUtf8()))
                          .apply(FileIO.readMatches())
                          .apply(ParquetIO.readFiles(schema))
                          .setCoder(AvroCoder.of(schema));
                    }
                  },
          TEST_PARQUET_WRITE_URN,
              spec ->
                  new PTransform<PCollection<GenericRecord>, PCollection<String>>() {
                    @Override
                    public PCollection<String> expand(PCollection<GenericRecord> input) {
                      return input
                          .apply(
                              FileIO.<GenericRecord>write()
                                  .via(ParquetIO.sink(schema))
                                  .to(spec.getPayload().toStringUtf8()))
                          .getPerDestinationOutputFilenames()
                          .apply(Values.create());
                    }
                  });
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    System.out.println("Starting expansion service at localhost:" + port);
    Server server = ServerBuilder.forPort(port).addService(new ExpansionService()).build();
    server.start();
    server.awaitTermination();
  }
}
