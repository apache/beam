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
package org.apache.beam.sdk.io.fileschematransform;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link FileWriteSchemaTransformFormatProviders} contains {@link
 * FileWriteSchemaTransformFormatProvider} implementations.
 *
 * <p>The design goals of this class are to enable clean {@link
 * FileWriteSchemaTransformConfiguration#getFormat()} lookups that map to the appropriate {@link
 * org.apache.beam.sdk.io.FileIO.Write} that encodes the file data into the configured format.
 */
@Internal
public final class FileWriteSchemaTransformFormatProviders {
  private static final String AVRO = "avro";
  private static final String CSV = "csv";
  private static final String JSON = "json";
  private static final String PARQUET = "parquet";
  private static final String XML = "xml";

  /** Load all {@link FileWriteSchemaTransformFormatProvider} implementations. */
  public static Map<String, FileWriteSchemaTransformFormatProvider> loadProviders() {
    return Providers.loadProviders(FileWriteSchemaTransformFormatProvider.class);
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for avro format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Avro implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return AVRO;
    }

    @Override
    public PTransform<PCollection<?>, PDone> buildTransform() {
      // TODO(https://github.com/apache/beam/issues/24472)
      throw new UnsupportedOperationException();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for CSV format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Csv implements FileWriteSchemaTransformFormatProvider {

    @Override
    public String identifier() {
      return CSV;
    }

    @Override
    public PTransform<PCollection<?>, PDone> buildTransform() {
      // TODO(https://github.com/apache/beam/issues/24472)
      throw new UnsupportedOperationException();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for JSON format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Json implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return JSON;
    }

    @Override
    public PTransform<PCollection<?>, PDone> buildTransform() {
      // TODO(https://github.com/apache/beam/issues/24472)
      throw new UnsupportedOperationException();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for Parquet format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Parquet implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return PARQUET;
    }

    @Override
    public PTransform<PCollection<?>, PDone> buildTransform() {
      // TODO(https://github.com/apache/beam/issues/24472)
      throw new UnsupportedOperationException();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for XML format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Xml implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return XML;
    }

    @Override
    public PTransform<PCollection<?>, PDone> buildTransform() {
      // TODO(https://github.com/apache/beam/issues/24472)
      throw new UnsupportedOperationException();
    }
  }
}
