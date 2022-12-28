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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class FileWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<FileWriteSchemaTransformConfiguration> {

  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:file_write:v1";
  private static final String INPUT_TAG = "input";

  @Override
  protected Class<FileWriteSchemaTransformConfiguration> configurationClass() {
    return FileWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(FileWriteSchemaTransformConfiguration configuration) {
    return new FileWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  static class FileWriteSchemaTransform extends PTransform<PCollectionRowTuple, PCollectionRowTuple>
      implements SchemaTransform {

    final FileWriteSchemaTransformConfiguration configuration;

    FileWriteSchemaTransform(FileWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (input.getAll().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "%s expects a single %s tagged PCollection<Row> input",
                FileWriteSchemaTransform.class.getName(), INPUT_TAG));
      }

      PCollection<Row> rowInput = input.get(INPUT_TAG);

      PTransform<PCollection<Row>, PDone> transform =
          getProvider().buildTransform(configuration, rowInput.getSchema());
      rowInput.apply(transform);

      return PCollectionRowTuple.empty(input.getPipeline());
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return this;
    }

    private FileWriteSchemaTransformFormatProvider getProvider() {
      Map<String, FileWriteSchemaTransformFormatProvider> providers =
          FileWriteSchemaTransformFormatProviders.loadProviders();
      if (!providers.containsKey(configuration.getFormat())) {
        throw new IllegalArgumentException(
            String.format(
                "%s is not a supported format. See %s for a list of supported formats.",
                configuration.getFormat(),
                FileWriteSchemaTransformFormatProviders.class.getName()));
      }
      // resolves [dereference.of.nullable]
      Optional<FileWriteSchemaTransformFormatProvider> provider =
          Optional.ofNullable(providers.get(configuration.getFormat()));
      checkState(provider.isPresent());
      return provider.get();
    }
  }
}
