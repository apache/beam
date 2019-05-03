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
package org.apache.beam.sdk.extensions.smb;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.SortedBucketSourceJoinBuilder;
import org.apache.beam.sdk.extensions.smb.avro.AvroSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.json.JsonSortedBucketIO;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** SMB join benchmark pipeline. */
public class SmbBenchmark {

  interface SourceOptions extends PipelineOptions {
    String getAvroSource();

    void setAvroSource(String value);

    String getJsonSource();

    void setJsonSource(String value);
  }

  public static void main(String[] args) throws IOException {
    SourceOptions sourceOptions = PipelineOptionsFactory.fromArgs(args).as(SourceOptions.class);
    Pipeline pipeline = Pipeline.create(sourceOptions);

    SortedBucketSource<String, KV<Iterable<AvroGeneratedUser>, Iterable<TableRow>>> source =
        SortedBucketSourceJoinBuilder.withFinalKeyType(String.class)
            .of(
                AvroSortedBucketIO.avroSource(
                    AvroGeneratedUser.class,
                    FileSystems.matchNewResource(sourceOptions.getAvroSource(), true)))
            .and(
                JsonSortedBucketIO.jsonSource(
                    FileSystems.matchNewResource(sourceOptions.getJsonSource(), true)))
            .build();

    pipeline
        .apply(source)
        .apply(
            FlatMapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(),
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(GenericRecord.class),
                            TypeDescriptor.of(TableRow.class))))
                .via(
                    kv -> {
                      String key = kv.getKey();
                      Iterable<AvroGeneratedUser> il = kv.getValue().getKey();
                      Iterable<TableRow> ir = kv.getValue().getValue();
                      List<KV<String, KV<GenericRecord, TableRow>>> output = new ArrayList<>();
                      for (GenericRecord l : il) {
                        for (TableRow r : ir) {
                          output.add(KV.of(key, KV.of(l, r)));
                        }
                      }
                      return output;
                    }))
        .setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                KvCoder.of(
                    AvroCoder.of(AvroGeneratedUser.getClassSchema()), TableRowJsonCoder.of())));

    pipeline.run();
  }
}
