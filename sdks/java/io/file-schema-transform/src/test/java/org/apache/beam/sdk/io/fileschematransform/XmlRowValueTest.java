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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.junit.Test;

/** Tests for {@link XmlRowValue}. */
public class XmlRowValueTest {

  @Test
  public void allPrimitiveDataTypes() {
    String aBoolean = "aBoolean";
    String aDecimal = "aDecimal";
    String aDouble = "aDouble";
    String aFloat = "aFloat";
    String anInteger = "anInteger";
    String aLong = "aLong";
    String aString = "aString";

    for (Row row : DATA.allPrimitiveDataTypesRows) {
      XmlRowValue aBooleanValue = new XmlRowValue();
      aBooleanValue.setValue(aBoolean, row);
      assertEquals(aBoolean, row.getValue(aBoolean), aBooleanValue.getPrimitiveValue());

      XmlRowValue aDecimalValue = new XmlRowValue();
      aDecimalValue.setValue(aDecimal, row);
      assertEquals(aDecimal, row.getValue(aDecimal), aDecimalValue.getPrimitiveValue());

      XmlRowValue aDoubleValue = new XmlRowValue();
      aDoubleValue.setValue(aDouble, row);
      assertEquals(aDouble, row.getValue(aDouble), aDoubleValue.getPrimitiveValue());

      XmlRowValue aFloatValue = new XmlRowValue();
      aFloatValue.setValue(aFloat, row);
      assertEquals(aFloat, row.getValue(aFloat), aFloatValue.getPrimitiveValue());

      XmlRowValue anIntegerValue = new XmlRowValue();
      anIntegerValue.setValue(anInteger, row);
      assertEquals(anInteger, row.getValue(anInteger), anIntegerValue.getPrimitiveValue());

      XmlRowValue aLongValue = new XmlRowValue();
      aLongValue.setValue(aLong, row);
      assertEquals(aLong, row.getValue(aLong), aLongValue.getPrimitiveValue());

      XmlRowValue aStringValue = new XmlRowValue();
      aStringValue.setValue(aString, row);
      assertEquals(aString, row.getValue(aString), aStringValue.getPrimitiveValue());
    }
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    String aBoolean = "aBoolean";
    String aDouble = "aDouble";
    String aFloat = "aFloat";
    String anInteger = "anInteger";
    String aLong = "aLong";
    String aString = "aString";
    for (Row row : DATA.nullableAllPrimitiveDataTypesRows) {
      XmlRowValue aBooleanValue = new XmlRowValue();
      aBooleanValue.setValue(aBoolean, row);
      assertEquals(aBoolean, row.getValue(aBoolean), aBooleanValue.getPrimitiveValue());

      XmlRowValue aDoubleValue = new XmlRowValue();
      aDoubleValue.setValue(aDouble, row);
      assertEquals(aDouble, row.getValue(aDouble), aDoubleValue.getPrimitiveValue());

      XmlRowValue aFloatValue = new XmlRowValue();
      aFloatValue.setValue(aFloat, row);
      assertEquals(aFloat, row.getValue(aFloat), aFloatValue.getPrimitiveValue());

      XmlRowValue anIntegerValue = new XmlRowValue();
      anIntegerValue.setValue(anInteger, row);
      assertEquals(anInteger, row.getValue(anInteger), anIntegerValue.getPrimitiveValue());

      XmlRowValue aLongValue = new XmlRowValue();
      aLongValue.setValue(aLong, row);
      assertEquals(aLong, row.getValue(aLong), aLongValue.getPrimitiveValue());

      XmlRowValue aStringValue = new XmlRowValue();
      aStringValue.setValue(aString, row);
      assertEquals(aString, row.getValue(aString), aStringValue.getPrimitiveValue());
    }
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    String booleans = "booleanList";
    String doubles = "doubleList";
    String floats = "floatList";
    String integers = "integerList";
    String longs = "longList";
    String strings = "stringList";

    for (Row row : DATA.arrayPrimitiveDataTypesRows) {
      XmlRowValue booleansValue = new XmlRowValue();
      booleansValue.setValue(booleans, row);
      Optional<ArrayList<XmlRowValue>> booleansValueList =
          Optional.ofNullable(booleansValue.getValueList());
      assertTrue(booleans, booleansValueList.isPresent());
      assertEquals(
          booleans,
          row.getArray(booleans),
          booleansValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));

      XmlRowValue doublesValue = new XmlRowValue();
      doublesValue.setValue(doubles, row);
      Optional<ArrayList<XmlRowValue>> doublesValueList =
          Optional.ofNullable(doublesValue.getValueList());
      assertTrue(doubles, doublesValueList.isPresent());
      assertEquals(
          doubles,
          row.getArray(doubles),
          doublesValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));

      XmlRowValue floatsValue = new XmlRowValue();
      floatsValue.setValue(floats, row);
      Optional<ArrayList<XmlRowValue>> floatsValueList =
          Optional.ofNullable(floatsValue.getValueList());
      assertTrue(floats, floatsValueList.isPresent());
      assertEquals(
          floats,
          row.getArray(floats),
          floatsValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));

      XmlRowValue integersValue = new XmlRowValue();
      integersValue.setValue(integers, row);
      Optional<ArrayList<XmlRowValue>> integersValueList =
          Optional.ofNullable(integersValue.getValueList());
      assertTrue(integers, integersValueList.isPresent());
      assertEquals(
          integers,
          row.getArray(integers),
          integersValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));

      XmlRowValue longsValue = new XmlRowValue();
      longsValue.setValue(longs, row);
      Optional<ArrayList<XmlRowValue>> longsValueList =
          Optional.ofNullable(longsValue.getValueList());
      assertTrue(longs, longsValueList.isPresent());
      assertEquals(
          longs,
          row.getArray(longs),
          longsValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));

      XmlRowValue stringsValue = new XmlRowValue();
      stringsValue.setValue(strings, row);
      Optional<ArrayList<XmlRowValue>> stringsValueList =
          Optional.ofNullable(stringsValue.getValueList());
      assertTrue(strings, stringsValueList.isPresent());
      assertEquals(
          strings,
          row.getArray(strings),
          stringsValueList.get().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void timeContaining() {
    String instant = "instant";
    String instantList = "instantList";
    for (Row row : DATA.timeContainingRows) {
      XmlRowValue instantValue = new XmlRowValue();
      instantValue.setValue(instant, row);
      Optional<ReadableDateTime> expected = Optional.ofNullable(row.getDateTime(instant));
      Optional<DateTime> actual = Optional.ofNullable(instantValue.getDateTimeValue());
      assertTrue(instant, expected.isPresent());
      assertTrue(instant, actual.isPresent());
      assertEquals(instant, expected.get().getMillis(), actual.get().getMillis());

      XmlRowValue instantListValue = new XmlRowValue();
      instantListValue.setValue(instantList, row);
      Optional<Collection<Instant>> expectedList = Optional.ofNullable(row.getArray(instantList));
      Optional<List<XmlRowValue>> actualList = Optional.ofNullable(instantListValue.getValueList());
      assertTrue(instantList, expectedList.isPresent());
      assertTrue(instantList, actualList.isPresent());
      assertFalse(instantList, expectedList.get().isEmpty());
      assertFalse(instantList, actualList.get().isEmpty());

      assertEquals(
          instantList,
          expectedList.get().stream().map(Instant::getMillis).collect(Collectors.toList()),
          dateTimes(actualList.get()).stream()
              .map(DateTime::getMillis)
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void byteType() {
    String singleByte = "byte";
    String byteList = "byteList";
    for (Row row : DATA.byteTypeRows) {
      XmlRowValue byteValue = new XmlRowValue();
      byteValue.setValue(singleByte, row);
      assertEquals(singleByte, row.getValue(singleByte), byteValue.getPrimitiveValue());

      XmlRowValue byteListValue = new XmlRowValue();
      byteListValue.setValue(byteList, row);
      assertEquals(
          singleByte,
          row.getArray(byteList),
          byteListValue.getValueList().stream()
              .map(XmlRowValue::getPrimitiveValue)
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    String allPrimitiveDataTypes = "allPrimitiveDataTypes";
    String allPrimitiveDataTypesList = "allPrimitiveDataTypesList";
    for (Row row : DATA.singlyNestedDataTypesNoRepeatRows) {
      XmlRowValue allPrimitiveDataTypesValue = new XmlRowValue();
      allPrimitiveDataTypesValue.setValue(allPrimitiveDataTypes, row);
      Optional<Row> expectedAllPrimitiveDataTypes =
          Optional.ofNullable(row.getRow(allPrimitiveDataTypes));
      Optional<Map<String, XmlRowValue>> actualAllPrimitiveDataTypes =
          Optional.ofNullable(allPrimitiveDataTypesValue.getNestedValue());
      assertTrue(allPrimitiveDataTypes, expectedAllPrimitiveDataTypes.isPresent());
      assertTrue(allPrimitiveDataTypes, actualAllPrimitiveDataTypes.isPresent());
      assertEquals(
          allPrimitiveDataTypes,
          values(expectedAllPrimitiveDataTypes.get()),
          values(actualAllPrimitiveDataTypes.get()));

      XmlRowValue allPrimitiveDataTypesValueList = new XmlRowValue();
      allPrimitiveDataTypesValueList.setValue(allPrimitiveDataTypesList, row);
      Optional<Collection<Row>> expectedAllPrimitiveDataTypesList =
          Optional.ofNullable(row.getArray(allPrimitiveDataTypesList));
      Optional<ArrayList<XmlRowValue>> actualAllPrimitiveDataTypesList =
          Optional.ofNullable(allPrimitiveDataTypesValueList.getValueList());

      assertTrue(allPrimitiveDataTypesList, expectedAllPrimitiveDataTypesList.isPresent());
      assertTrue(allPrimitiveDataTypesList, actualAllPrimitiveDataTypesList.isPresent());
      assertTrue(allPrimitiveDataTypesList, expectedAllPrimitiveDataTypesList.get().isEmpty());
      assertTrue(allPrimitiveDataTypesList, actualAllPrimitiveDataTypesList.get().isEmpty());
    }
  }

  @Test
  public void singlyNestedDataTypesRepeat() {
    String allPrimitiveDataTypes = "allPrimitiveDataTypes";
    String allPrimitiveDataTypesList = "allPrimitiveDataTypesList";
    for (Row row : DATA.singlyNestedDataTypesRepeatedRows) {
      XmlRowValue allPrimitiveDataTypesValue = new XmlRowValue();
      allPrimitiveDataTypesValue.setValue(allPrimitiveDataTypes, row);
      Optional<Row> expectedAllPrimitiveDataTypes =
          Optional.ofNullable(row.getRow(allPrimitiveDataTypes));
      Optional<Map<String, XmlRowValue>> actualAllPrimitiveDataTypes =
          Optional.ofNullable(allPrimitiveDataTypesValue.getNestedValue());
      assertTrue(allPrimitiveDataTypes, expectedAllPrimitiveDataTypes.isPresent());
      assertTrue(allPrimitiveDataTypes, actualAllPrimitiveDataTypes.isPresent());
      assertEquals(
          allPrimitiveDataTypes,
          values(expectedAllPrimitiveDataTypes.get()),
          values(actualAllPrimitiveDataTypes.get()));

      XmlRowValue allPrimitiveDataTypesValueList = new XmlRowValue();
      allPrimitiveDataTypesValueList.setValue(allPrimitiveDataTypesList, row);
      Optional<Collection<Row>> expectedAllPrimitiveDataTypesList =
          Optional.ofNullable(row.getArray(allPrimitiveDataTypesList));
      Optional<List<XmlRowValue>> actualAllPrimitiveDataTypesList =
          Optional.ofNullable(allPrimitiveDataTypesValueList.getValueList());

      assertTrue(allPrimitiveDataTypesList, expectedAllPrimitiveDataTypesList.isPresent());
      assertTrue(allPrimitiveDataTypesList, actualAllPrimitiveDataTypesList.isPresent());
      assertFalse(allPrimitiveDataTypesList, expectedAllPrimitiveDataTypesList.get().isEmpty());
      assertFalse(allPrimitiveDataTypesList, actualAllPrimitiveDataTypesList.get().isEmpty());
      assertEquals(
          allPrimitiveDataTypesList,
          valuesList(expectedAllPrimitiveDataTypesList.get()),
          valuesList(actualAllPrimitiveDataTypesList.get()));
    }
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    String singlyNestedDataTypes = "singlyNestedDataTypes";
    String allPrimitiveDataTypes = "allPrimitiveDataTypes";
    String allPrimitiveDataTypesList = "allPrimitiveDataTypesList";
    for (Row row : DATA.doublyNestedDataTypesNoRepeatRows) {
      XmlRowValue singlyNestedDataTypesValue = new XmlRowValue();
      singlyNestedDataTypesValue.setValue(singlyNestedDataTypes, row);
      Optional<Row> expectedSinglyNestedDataTypesValue =
          Optional.ofNullable(row.getRow(singlyNestedDataTypes));
      Optional<Map<String, XmlRowValue>> actualSinglyNestedDataTypesValue =
          Optional.ofNullable(singlyNestedDataTypesValue.getNestedValue());
      assertTrue(singlyNestedDataTypes, expectedSinglyNestedDataTypesValue.isPresent());
      assertTrue(singlyNestedDataTypes, actualSinglyNestedDataTypesValue.isPresent());
      assertNotNull(
          singlyNestedDataTypes,
          expectedSinglyNestedDataTypesValue.get().getValue(allPrimitiveDataTypes));
      assertNotNull(
          singlyNestedDataTypes, actualSinglyNestedDataTypesValue.get().get(allPrimitiveDataTypes));
      assertNotNull(
          singlyNestedDataTypes,
          expectedSinglyNestedDataTypesValue.get().getValue(allPrimitiveDataTypesList));
      assertNotNull(
          singlyNestedDataTypes,
          actualSinglyNestedDataTypesValue.get().get(allPrimitiveDataTypesList));
    }
  }

  private static Map<String, Object> values(Row row) {
    Map<String, Object> result = new HashMap<>();
    Schema schema = row.getSchema();
    for (String key : schema.getFieldNames()) {
      result.put(key, row.getValue(key));
    }
    return result;
  }

  private static List<Map<String, Object>> valuesList(Collection<Row> values) {
    return values.stream().map(XmlRowValueTest::values).collect(Collectors.toList());
  }

  private static Map<String, Object> values(Map<String, XmlRowValue> nested) {
    Map<String, Object> result = new HashMap<>();
    for (String key : nested.keySet()) {
      result.put(key, nested.get(key).getPrimitiveValue());
    }
    return result;
  }

  private static List<Map<String, Object>> valuesList(List<XmlRowValue> nestedList) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (XmlRowValue item : nestedList) {
      Optional<Map<String, XmlRowValue>> nestedValues = Optional.ofNullable(item.getNestedValue());
      nestedValues.ifPresent(stringXmlRowValueMap -> result.add(values(stringXmlRowValueMap)));
    }
    return result;
  }

  private static List<DateTime> dateTimes(List<XmlRowValue> dateTimeValues) {
    List<DateTime> result = new ArrayList<>();
    for (XmlRowValue item : dateTimeValues) {
      Optional<DateTime> value = Optional.ofNullable(item.getDateTimeValue());
      value.ifPresent(result::add);
    }
    return result;
  }
}
