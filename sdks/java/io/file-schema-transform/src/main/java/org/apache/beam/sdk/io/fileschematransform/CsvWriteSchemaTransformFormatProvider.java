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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** A {@link FileWriteSchemaTransformFormatProvider} for CSV format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class CsvWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  @Override
  public String identifier() {
    return FileWriteSchemaTransformFormatProviders.CSV;
  }

  @Override
  public PTransform<PCollection<Row>, PCollection<String>> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {
    // TODO(https://github.com/apache/beam/issues/24469)
    throw new UnsupportedOperationException();
  }
}
