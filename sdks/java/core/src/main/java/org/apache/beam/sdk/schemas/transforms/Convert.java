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
package org.apache.beam.sdk.schemas.transforms;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A set of utilities for converting between different objects supporting schemas. */
@Experimental(Kind.SCHEMAS)
public class Convert {
  /**
   * Convert a {@link PCollection}{@literal <InputT>} into a {@link PCollection}{@literal <Row>}.
   *
   * <p>The input {@link PCollection} must have a schema attached. The output collection will have
   * the same schema as the iput.
   */
  public static <InputT> PTransform<PCollection<InputT>, PCollection<Row>> toRows() {
    return to(Row.class);
  }

  /**
   * Convert a {@link PCollection}{@literal <Row>} into a {@link PCollection}{@literal <OutputT>}.
   *
   * <p>The output schema will be inferred using the schema registry. A schema must be registered
   * for this type, or the conversion will fail.
   */
  public static <OutputT> PTransform<PCollection<Row>, PCollection<OutputT>> fromRows(
      Class<OutputT> clazz) {
    return to(clazz);
  }

  /**
   * Convert a {@link PCollection}{@literal <Row>} into a {@link PCollection}{@literal <Row>}.
   *
   * <p>The output schema will be inferred using the schema registry. A schema must be registered
   * for this type, or the conversion will fail.
   */
  public static <OutputT> PTransform<PCollection<Row>, PCollection<OutputT>> fromRows(
      TypeDescriptor<OutputT> typeDescriptor) {
    return to(typeDescriptor);
  }

  /**
   * Convert a {@link PCollection}{@literal <InputT>} to a {@link PCollection}{@literal <OutputT>}.
   *
   * <p>This function allows converting between two types as long as the two types have
   * <i>compatible</i> schemas. Two schemas are said to be <i>compatible</i> if they recursively
   * have fields with the same names, but possibly different orders.
   */
  public static <InputT, OutputT> PTransform<PCollection<InputT>, PCollection<OutputT>> to(
      Class<OutputT> clazz) {
    return to(TypeDescriptor.of(clazz));
  }

  /**
   * Convert a {@link PCollection}{@literal <InputT>} to a {@link PCollection}{@literal <OutputT>}.
   *
   * <p>This function allows converting between two types as long as the two types have
   * <i>compatible</i> schemas. Two schemas are said to be <i>compatible</i> if they recursively
   * have fields with the same names, but possibly different orders.
   */
  public static <InputT, OutputT> PTransform<PCollection<InputT>, PCollection<OutputT>> to(
      TypeDescriptor<OutputT> typeDescriptor) {
    return new ConvertTransform<>(typeDescriptor);
  }

  private static class ConvertTransform<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    TypeDescriptor<OutputT> outputTypeDescriptor;

    ConvertTransform(TypeDescriptor<OutputT> outputTypeDescriptor) {
      this.outputTypeDescriptor = outputTypeDescriptor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      if (!input.hasSchema()) {
        throw new RuntimeException("Convert requires a schema on the input.");
      }

      final SchemaCoder<OutputT> outputSchemaCoder;
      boolean toRow = outputTypeDescriptor.equals(TypeDescriptor.of(Row.class));
      if (toRow) {
        // If the output is of type Row, then just forward the schema of the input type to the
        // output.
        outputSchemaCoder =
            (SchemaCoder<OutputT>)
                SchemaCoder.of(
                    input.getSchema(),
                    SerializableFunctions.identity(),
                    SerializableFunctions.identity());
      } else {
        // Otherwise, try to find a schema for the output type in the schema registry.
        SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
        try {
          outputSchemaCoder =
              SchemaCoder.of(
                  registry.getSchema(outputTypeDescriptor),
                  registry.getToRowFunction(outputTypeDescriptor),
                  registry.getFromRowFunction(outputTypeDescriptor));
          // assert matches input schema.
          // TODO: Properly handle nullable.
          if (!outputSchemaCoder.getSchema().assignableToIgnoreNullable(input.getSchema())) {
            throw new RuntimeException(
                "Cannot convert between types that don't have equivalent schemas."
                    + " input schema: "
                    + input.getSchema()
                    + " output schema: "
                    + outputSchemaCoder.getSchema());
          }
        } catch (NoSuchSchemaException e) {
          throw new RuntimeException("No schema registered for " + outputTypeDescriptor);
        }
      }

      return input
          .apply(
              ParDo.of(
                  new DoFn<InputT, OutputT>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<OutputT> o) {
                      o.output(outputSchemaCoder.getFromRowFunction().apply(row));
                    }
                  }))
          .setSchema(
              outputSchemaCoder.getSchema(),
              outputSchemaCoder.getToRowFunction(),
              outputSchemaCoder.getFromRowFunction());
    }
  }
}
