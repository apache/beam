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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;

/** A {@link Coder} for {@link FileIO.ReadableFile}. */
public class ReadableFileCoder extends AtomicCoder<FileIO.ReadableFile> {
  private static final ReadableFileCoder INSTANCE = new ReadableFileCoder();

  private static final BooleanCoder IS_SEEKABLE_CODER = BooleanCoder.of();
  private static final VarIntCoder COMPRESSION_CODER = VarIntCoder.of();
  private static final ResourceIdCoder RESOURCE_ID_CODER = ResourceIdCoder.of();
  private static final VarLongCoder SIZE_CODER = VarLongCoder.of();

  /** Returns the instance of {@link ReadableFileCoder}. */
  public static ReadableFileCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(FileIO.ReadableFile value, OutputStream os) throws IOException {
    RESOURCE_ID_CODER.encode(value.getResourceId(), os);
    SIZE_CODER.encode(value.getSizeBytes(), os);
    IS_SEEKABLE_CODER.encode(value.isSeekable(), os);
    COMPRESSION_CODER.encode(value.getCompression().ordinal(), os);
  }

  @Override
  public FileIO.ReadableFile decode(InputStream is) throws IOException {
    ResourceId resourceId = RESOURCE_ID_CODER.decode(is);
    long sizeBytes = SIZE_CODER.decode(is);
    boolean isSeekable = IS_SEEKABLE_CODER.decode(is);
    Compression compression = Compression.values()[COMPRESSION_CODER.decode(is)];
    return new FileIO.ReadableFile(resourceId, sizeBytes, isSeekable, compression);
  }
}
