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
package org.apache.beam.io.iceberg;

import org.apache.beam.io.iceberg.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

@SuppressWarnings("all")
public class WriteGroupedRecordsToFiles<DestinationT, ElementT>
    extends DoFn<KV<DestinationT, Iterable<ElementT>>, Result<DestinationT>> {

  private final PCollectionView<String> locationPrefixView;
  private final long maxFileSize;
  private final RecordWriterFactory<ElementT, DestinationT> recordWriterFactory;

  WriteGroupedRecordsToFiles(
      PCollectionView<String> locationPrefixView,
      long maxFileSize,
      RecordWriterFactory<ElementT, DestinationT> recordWriterFactory) {
    this.locationPrefixView = locationPrefixView;
    this.maxFileSize = maxFileSize;
    this.recordWriterFactory = recordWriterFactory;
  }

  @ProcessElement
  public void processElement(
      ProcessContext c, @Element KV<DestinationT, Iterable<ElementT>> element) throws Exception {
    String locationPrefix = c.sideInput(locationPrefixView);
    DestinationT destination = element.getKey();
    RecordWriter<ElementT> writer = recordWriterFactory.createWriter(locationPrefix, destination);
    for (ElementT e : element.getValue()) {
      writer.write(e);
    }
    writer.close();
    c.output(
        new Result<>(
            writer.table().name(),
            writer.location(),
            writer.dataFile(),
            writer.table().spec(),
            destination));
  }
}
