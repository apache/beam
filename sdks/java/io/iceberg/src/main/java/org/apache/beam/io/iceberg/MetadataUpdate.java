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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.AvroEncoderUtil;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.StructType;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@SuppressWarnings("all")
public class MetadataUpdate implements IndexedRecord, SchemaConstructable {

  private List<DataFile> dataFiles;
  private List<DeleteFile> deleteFiles;

  private final Schema avroSchema;

  public MetadataUpdate(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public MetadataUpdate(
      StructType partitionType, List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;

    StructType dataFileStruct = DataFile.getType(partitionType);
    Map<StructType, String> dataFileNames =
        ImmutableMap.of(
            dataFileStruct, "org.apache.iceberg.GenericDataFile",
            partitionType, "org.apache.iceberg.PartitionData");
    Schema dataFileSchema = AvroSchemaUtil.convert(dataFileStruct, dataFileNames);
    Map<StructType, String> deleteFileNames =
        ImmutableMap.of(
            dataFileStruct, "org.apache.iceberg.GenericDeleteFile",
            partitionType, "org.apache.iceberg.PartitionData");
    Schema deleteFileSchema = AvroSchemaUtil.convert(dataFileStruct, deleteFileNames);

    this.avroSchema =
        SchemaBuilder.builder()
            .record(getClass().getName())
            .fields()
            .name("dataFiles")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, "-1")
            .type()
            .nullable()
            .array()
            .items(dataFileSchema)
            .noDefault()
            .name("deleteFiles")
            .prop(AvroSchemaUtil.FIELD_ID_PROP, "-1")
            .type()
            .nullable()
            .array()
            .items(deleteFileSchema)
            .noDefault()
            .endRecord();
  }

  public static MetadataUpdate of(PartitionSpec partitionSpec, DataFile dataFile) {
    return new MetadataUpdate(partitionSpec.partitionType(), ImmutableList.of(dataFile), null);
  }

  public List<DataFile> getDataFiles() {
    return this.dataFiles;
  }

  public List<DeleteFile> getDeleteFiles() {
    return this.deleteFiles;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.dataFiles = (List<DataFile>) v;
        return;
      case 1:
        this.deleteFiles = (List<DeleteFile>) v;
        return;
      default:
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return this.dataFiles;
      case 1:
        return this.deleteFiles;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  protected static class MetadataUpdateCoder extends Coder<MetadataUpdate> {

    private static final ByteArrayCoder bytesCoder = ByteArrayCoder.of();

    @Override
    public void encode(
        MetadataUpdate value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      bytesCoder.encode(AvroEncoderUtil.encode(value, value.getSchema()), outStream);
    }

    @Override
    public MetadataUpdate decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      byte[] updateBytes = bytesCoder.decode(inStream);
      return AvroEncoderUtil.decode(updateBytes);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<
            ? extends
                @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>>
        getCoderArguments() {
      return ImmutableList.of();
    }

    @Override
    public void verifyDeterministic()
        throws @UnknownKeyFor @NonNull @Initialized NonDeterministicException {}
  }

  public static Coder<MetadataUpdate> coder() {
    return new MetadataUpdateCoder();
  }
}
