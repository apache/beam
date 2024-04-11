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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
@DefaultCoder(FileWriteResult.FileWriteResultCoder.class)
abstract class FileWriteResult {
  public abstract TableIdentifier getTableIdentifier();

  public abstract PartitionSpec getPartitionSpec();

  public abstract DataFile getDataFile();

  public static Builder builder() {
    return new AutoValue_FileWriteResult.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder setTableIdentifier(TableIdentifier tableId);

    public abstract Builder setPartitionSpec(PartitionSpec partitionSpec);

    public abstract Builder setDataFile(DataFile dataFiles);

    public abstract FileWriteResult build();
  }

  public static class FileWriteResultCoder extends StructuredCoder<FileWriteResult> {
    private static final FileWriteResultCoder SINGLETON = new FileWriteResultCoder();

    private static final Coder<String> tableIdentifierCoder = StringUtf8Coder.of();
    private static final Coder<PartitionSpec> partitionSpecCoder =
        SerializableCoder.of(PartitionSpec.class);
    private static final Coder<byte[]> dataFileBytesCoder = ByteArrayCoder.of();

    private static Schema getDataFileAvroSchema(FileWriteResult fileWriteResult) {
      Types.StructType partitionType = fileWriteResult.getPartitionSpec().partitionType();
      Types.StructType dataFileStruct = DataFile.getType(partitionType);
      Map<Types.StructType, String> dataFileNames =
          ImmutableMap.of(
              dataFileStruct, "org.apache.iceberg.GenericDataFile",
              partitionType, "org.apache.iceberg.PartitionData");
      return AvroSchemaUtil.convert(dataFileStruct, dataFileNames);
    }

    @Override
    public void encode(FileWriteResult value, OutputStream outStream)
        throws CoderException, IOException {
      tableIdentifierCoder.encode(value.getTableIdentifier().toString(), outStream);
      partitionSpecCoder.encode(value.getPartitionSpec(), outStream);
      dataFileBytesCoder.encode(
          AvroEncoderUtil.encode(value.getDataFile(), getDataFileAvroSchema(value)), outStream);
    }

    @Override
    public FileWriteResult decode(InputStream inStream) throws CoderException, IOException {
      TableIdentifier tableId = TableIdentifier.parse(tableIdentifierCoder.decode(inStream));
      PartitionSpec partitionSpec = partitionSpecCoder.decode(inStream);
      DataFile dataFile =
          checkArgumentNotNull(
              AvroEncoderUtil.decode(dataFileBytesCoder.decode(inStream)),
              "Decoding of dataFile resulted in null");
      return FileWriteResult.builder()
          .setTableIdentifier(tableId)
          .setDataFile(dataFile)
          .setPartitionSpec(partitionSpec)
          .build();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public Object structuralValue(FileWriteResult fileWriteResult) {
      return new FileWriteResultDeepEqualityWrapper(fileWriteResult);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public TypeDescriptor<FileWriteResult> getEncodedTypeDescriptor() {
      return TypeDescriptor.of(FileWriteResult.class);
    }

    public static FileWriteResultCoder of() {
      return SINGLETON;
    }

    @SuppressWarnings("unused") // used via `DefaultCoder` annotation
    public static CoderProvider getCoderProvider() {
      return CoderProviders.forCoder(
          TypeDescriptor.of(FileWriteResult.class), FileWriteResultCoder.of());
    }
  }

  private static class FileWriteResultDeepEqualityWrapper {
    private final FileWriteResult fileWriteResult;

    private FileWriteResultDeepEqualityWrapper(FileWriteResult fileWriteResult) {
      this.fileWriteResult = fileWriteResult;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof FileWriteResultDeepEqualityWrapper)) {
        return false;
      }
      FileWriteResultDeepEqualityWrapper other = (FileWriteResultDeepEqualityWrapper) obj;

      return Objects.equals(
              fileWriteResult.getTableIdentifier(), other.fileWriteResult.getTableIdentifier())
          && Objects.equals(
              fileWriteResult.getPartitionSpec(), other.fileWriteResult.getPartitionSpec())
          && dataFilesEqual(fileWriteResult.getDataFile(), other.fileWriteResult.getDataFile());
    }

    private boolean dataFilesEqual(DataFile first, DataFile second) {
      return Objects.equals(first.pos(), second.pos())
          && first.specId() == second.specId()
          && Objects.equals(first.content(), second.content())
          && Objects.equals(first.path(), second.path())
          && Objects.equals(first.format(), second.format())
          && Objects.equals(first.partition(), second.partition())
          && first.recordCount() == second.recordCount()
          && first.fileSizeInBytes() == second.fileSizeInBytes()
          && Objects.equals(first.columnSizes(), second.columnSizes())
          && Objects.equals(first.valueCounts(), second.valueCounts())
          && Objects.equals(first.nullValueCounts(), second.nullValueCounts())
          && Objects.equals(first.nanValueCounts(), second.nanValueCounts())
          && Objects.equals(first.lowerBounds(), second.lowerBounds())
          && Objects.equals(first.upperBounds(), second.upperBounds())
          && Objects.equals(first.keyMetadata(), second.keyMetadata())
          && Objects.equals(first.splitOffsets(), second.splitOffsets())
          && Objects.equals(first.equalityFieldIds(), second.equalityFieldIds())
          && Objects.equals(first.sortOrderId(), second.sortOrderId())
          && Objects.equals(first.dataSequenceNumber(), second.dataSequenceNumber())
          && Objects.equals(first.fileSequenceNumber(), second.fileSequenceNumber());
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          fileWriteResult.getTableIdentifier(),
          fileWriteResult.getPartitionSpec(),
          fileWriteResult.getDataFile());
    }
  }
}
