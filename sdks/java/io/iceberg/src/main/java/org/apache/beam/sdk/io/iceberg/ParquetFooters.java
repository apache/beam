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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO.ReadFiles.BeamParquetInputFile;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;

/** Reads Parquet footers through Beam's {@link FileSystems}, so no table or FileIO is needed. */
final class ParquetFooters {
  private ParquetFooters() {}

  static ParquetMetadata read(String filePath) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile(filePath))) {
      return reader.getFooter();
    }
  }

  private static InputFile inputFile(String filePath) throws IOException {
    ResourceId resourceId =
        Iterables.getOnlyElement(FileSystems.match(filePath).metadata()).resourceId();
    Compression compression = Compression.detect(checkStateNotNull(resourceId.getFilename()));
    SeekableByteChannel channel =
        (SeekableByteChannel) compression.readDecompressed(FileSystems.open(resourceId));
    return new BeamParquetInputFile(channel);
  }
}
