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
import java.math.BigDecimal;
import java.util.HashMap;
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
import org.joda.time.base.AbstractInstant;

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

  public <OutputT> CsvIOParse<T> withCustomRecordParsing(
      String fieldName,
      SerializableFunction<String, Object> customRecordParsingFn,
      Class<OutputT> outputClass) {

    validateCustomRecordParsingFn(getConfigBuilder().build().getSchema(), fieldName, outputClass);

    Map<String, SerializableFunction<String, Object>> customProcessingMap = new HashMap<>();
    if (getConfigBuilder().getCustomProcessingMap().isPresent()) {
      customProcessingMap.putAll(getConfigBuilder().getCustomProcessingMap().get());
    }

    customProcessingMap.put(fieldName, customRecordParsingFn);
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
    //        Schema schema = configuration.getSchema();
    //        Map<String, SerializableFunction<String, Object>> customProcessingMap =
    //     configuration.getCustomProcessingMap();
    //        Map<String, Object> allTypes = new HashMap<>();
    //        for(Schema.Field field : schema.getFields()) {
    //          allTypes.put(field.getName(), field.getType());
    //        }
    //        for(Map.Entry<String, SerializableFunction<String, Object>> entry :
    //     customProcessingMap.entrySet()) {
    //            Object checkingClass = allTypes.get(entry.getKey());
    //    //      Object checkingClass = entry.getValue().apply(); // how to get string from
    ////     serializable to put into apply
    //          // need to access object by itself
    //          String fieldName = entry.getKey();
    //          // problems with getting class, says class is unknown and connot compare the class
    //          Class<T> parseClass = (schema).getField(fieldName).getType().getClass();
    //          if(!parseClass.isInstance(checkingClass)) {
    //            throw new IllegalArgumentException("Custom parsing type mismatch with Serializable
    // Function");
    //          }
    //
    //    //      // could this potentially have different values?
    //    //      if(!((value.hashCode()) == parseClass.hashCode())){
    //    //        throw new IllegalArgumentException("Custom parsing type mismatch with
    // Serializable
    ////     Function");
    //    //      }
    //        }
    //

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

  private static <OutputT> void validateCustomRecordParsingFn(
      Schema schema, String fieldName, Class<OutputT> outputClass) {
    Schema.Field field = schema.getField(fieldName);
    Schema.TypeName typeName = field.getType().getTypeName();

    if ((typeName.equals(Schema.TypeName.STRING) && !outputClass.isAssignableFrom(String.class))
        || (typeName.equals(Schema.TypeName.INT16) && !outputClass.isAssignableFrom(Short.class))
        || (typeName.equals(Schema.TypeName.INT32) && !outputClass.isAssignableFrom(Integer.class))
        || (typeName.equals(Schema.TypeName.INT64) && !outputClass.isAssignableFrom(Long.class))
        || (typeName.equals(Schema.TypeName.BOOLEAN)
            && !outputClass.isAssignableFrom(Boolean.class))
        || (typeName.equals(Schema.TypeName.BYTE) && !outputClass.isAssignableFrom(Byte.class))
        || (typeName.equals(Schema.TypeName.DECIMAL)
            && !outputClass.isAssignableFrom(BigDecimal.class))
        || (typeName.equals(Schema.TypeName.DOUBLE) && !outputClass.isAssignableFrom(Double.class))
        || (typeName.equals(Schema.TypeName.FLOAT) && !outputClass.isAssignableFrom(Float.class))
        || (typeName.equals(Schema.TypeName.DATETIME)
            && !outputClass.isAssignableFrom(AbstractInstant.class))) {
      throw new IllegalArgumentException(
          String.format(
              "Schema.FieldType %s does not support the output class %s of the custom record parsing function",
              typeName, outputClass));
    }
  }
}
