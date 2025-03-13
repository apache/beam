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
package org.apache.beam.sdk.io.csv;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * {@link PTransform} for Parsing CSV Record Strings into {@link Schema}-mapped target types. {@link
 * CsvIOParse} is not instantiated directly but via {@link CsvIO#parse} or {@link CsvIO#parseRows}.
 */
@AutoValue
public abstract class CsvIOParse<T> extends PTransform<PCollection<String>, CsvIOParseResult<T>> {

  final TupleTag<T> outputTag = new TupleTag<T>() {};
  final TupleTag<CsvIOParseError> errorTag = new TupleTag<CsvIOParseError>() {};

  static <T> CsvIOParse.Builder<T> builder() {
    return new AutoValue_CsvIOParse.Builder<>();
  }

  /**
   * Configures custom cell parsing.
   *
   * <h2>Example</h2>
   *
   * <pre>{@code
   * CsvIO.parse().withCustomRecordParsing("listOfInts", cell-> {
   *
   *  List<Integer> result = new ArrayList<>();
   *  for (String stringValue: Splitter.on(";").split(cell)) {
   *    result.add(Integer.parseInt(stringValue));
   *  }
   *
   * });
   * }</pre>
   */
  public <OutputT extends @NonNull Object> CsvIOParse<T> withCustomRecordParsing(
      String fieldName, SerializableFunction<String, OutputT> customRecordParsingFn) {

    Map<String, SerializableFunction<String, Object>> customProcessingMap =
        getConfigBuilder().getOrCreateCustomProcessingMap();

    customProcessingMap.put(fieldName, customRecordParsingFn::apply);
    getConfigBuilder().setCustomProcessingMap(customProcessingMap);
    return this;
  }

  /** Contains all configuration parameters for {@link CsvIOParse}. */
  abstract CsvIOParseConfiguration.Builder<T> getConfigBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setConfigBuilder(CsvIOParseConfiguration.Builder<T> configBuilder);

    abstract CsvIOParse<T> build();
  }

  @Override
  public CsvIOParseResult<T> expand(PCollection<String> input) {
    CsvIOParseConfiguration<T> configuration = getConfigBuilder().build();

    CsvIOStringToCsvRecord stringToCsvRecord =
        new CsvIOStringToCsvRecord(configuration.getCsvFormat());
    CsvIOParseResult<List<String>> stringToCsvRecordResult = input.apply(stringToCsvRecord);
    PCollection<List<String>> stringToRecordOutput = stringToCsvRecordResult.getOutput();
    PCollection<CsvIOParseError> stringToRecordErrors = stringToCsvRecordResult.getErrors();

    CsvIORecordToObjects<T> recordToObjects = new CsvIORecordToObjects<T>(configuration);
    CsvIOParseResult<T> recordToObjectsResult = stringToRecordOutput.apply(recordToObjects);
    PCollection<T> output = recordToObjectsResult.getOutput();
    PCollection<CsvIOParseError> recordToObjectsErrors = recordToObjectsResult.getErrors();

    PCollectionList<CsvIOParseError> errorList =
        PCollectionList.of(stringToRecordErrors).and(recordToObjectsErrors);
    PCollection<CsvIOParseError> errors = errorList.apply(Flatten.pCollections());

    PCollectionTuple result = PCollectionTuple.of(outputTag, output).and(errorTag, errors);
    return CsvIOParseResult.of(outputTag, configuration.getCoder(), errorTag, result);
  }
}
