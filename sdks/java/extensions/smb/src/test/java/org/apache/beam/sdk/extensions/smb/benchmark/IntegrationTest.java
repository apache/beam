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
package org.apache.beam.sdk.extensions.smb.benchmark;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.smb.benchmark.SinkBenchmark.SinkOptions;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.MoreFiles;

/** Integration Test. */
public class IntegrationTest {
  public static void main(String[] args) throws IOException {
    final Path temp = Files.createTempDirectory("smb-");
    final String[] sinkArgs = {
      "--avroPath=" + temp.resolve("avro"),
      "--jsonPath=" + temp.resolve("json"),
      "--tempLocation=" + temp.resolve("temp"),
      "--numKeys=1000",
      "--maxRecordsPerKey=20",
      "--avroNumBuckets=16",
      "--avroNumShards=4",
      "--jsonNumBuckets=8",
      "--jsonNumShards=2"
    };

    final SinkOptions options = PipelineOptionsFactory.fromArgs(sinkArgs).as(SinkOptions.class);
    run(options);

    MoreFiles.deleteRecursively(temp);
  }

  static void run(PipelineOptions options) {
    // Write sorted-bucket files and wait
    SinkBenchmark.write(Pipeline.create(options)).waitUntilFinish();

    final Pipeline p = Pipeline.create(options);

    // Read with SortedBucketSource
    final PCollection<KV<String, Iterable<KV<AvroGeneratedUser, TableRow>>>> smbResult =
        SourceBenchmark.read(p).apply(GroupByKey.create());

    // Read separately and join with CoGroupByKey
    final PCollection<KV<String, Iterable<KV<AvroGeneratedUser, TableRow>>>> coGbkResult =
        CoGroupByKeyBenchmark.read(p).apply(GroupByKey.create());

    // CoGroupByKey 2 results and compare
    final TupleTag<Iterable<KV<AvroGeneratedUser, TableRow>>> lhsTag = new TupleTag<>();
    final TupleTag<Iterable<KV<AvroGeneratedUser, TableRow>>> rhsTag = new TupleTag<>();
    final PCollection<Boolean> equals =
        KeyedPCollectionTuple.of(lhsTag, smbResult)
            .and(rhsTag, coGbkResult)
            .apply(CoGroupByKey.create())
            .apply(
                MapElements.into(TypeDescriptors.booleans())
                    .via(
                        kv -> {
                          final List<Iterable<KV<AvroGeneratedUser, TableRow>>> lList =
                              Lists.newArrayList(kv.getValue().getAll(lhsTag));
                          final List<Iterable<KV<AvroGeneratedUser, TableRow>>> rList =
                              Lists.newArrayList(kv.getValue().getAll(rhsTag));
                          Preconditions.checkState(lList.size() == 1);
                          Preconditions.checkState(rList.size() == 1);

                          final Set<KV<AvroGeneratedUser, TableRow>> lSet =
                              Sets.newHashSet(lList.get(0));
                          final Set<KV<AvroGeneratedUser, TableRow>> rSet =
                              Sets.newHashSet(rList.get(0));
                          return lSet.equals(rSet);
                        }))
            .apply(Combine.globally((x, y) -> x && y));

    PAssert.thatSingleton(equals).isEqualTo(true);

    p.run();
  }
}
