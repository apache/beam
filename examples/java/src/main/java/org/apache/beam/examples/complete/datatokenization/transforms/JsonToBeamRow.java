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
package org.apache.beam.examples.complete.datatokenization.transforms;

import static org.apache.beam.examples.complete.datatokenization.DataTokenization.FAILSAFE_ELEMENT_CODER;

import org.apache.beam.examples.complete.datatokenization.utils.ErrorConverters;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElement;
import org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** The {@link JsonToBeamRow} converts jsons string to beam rows. */
public class JsonToBeamRow extends PTransform<PCollection<String>, PCollection<Row>> {

  private final String failedToParseDeadLetterPath;
  private final transient SchemasUtils schema;

  public JsonToBeamRow(String failedToParseDeadLetterPath, SchemasUtils schema) {
    this.failedToParseDeadLetterPath = failedToParseDeadLetterPath;
    this.schema = schema;
  }

  @Override
  @SuppressWarnings("argument")
  public PCollection<Row> expand(PCollection<String> jsons) {
    ParseResult rows =
        jsons.apply(
            "JsonToRow",
            JsonToRow.withExceptionReporting(schema.getBeamSchema()).withExtendedErrorInfo());

    if (failedToParseDeadLetterPath != null) {
      /*
       * Write Row conversion errors to filesystem specified path
       */
      rows.getFailedToParseLines()
          .apply(
              "ToFailsafeElement",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via(
                      (Row errRow) ->
                          FailsafeElement.of(
                                  Strings.nullToEmpty(errRow.getString("line")),
                                  Strings.nullToEmpty(errRow.getString("line")))
                              .setErrorMessage(Strings.nullToEmpty(errRow.getString("err")))))
          .apply(
              "WriteCsvConversionErrorsToFS",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(failedToParseDeadLetterPath)
                  .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                  .build());
    }

    return rows.getResults().setRowSchema(schema.getBeamSchema());
  }
}
