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
package org.apache.beam.examples.snippets.transforms.io.gcp.bigquery;

// [START bigquery_my_data]

import com.google.api.services.bigquery.model.TableRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

class BigQueryMyData {

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  static class MyStruct {
    String stringValue;
    Long int64Value;
  }

  @DefaultCoder(AvroCoder.class)
  static class MyData {
    String myString;
    Long myInt64;
    Double myFloat64;
    BigDecimal myNumeric;
    Boolean myBoolean;
    byte[] myBytes;
    String myDate;
    String myDateTime;
    String myTime;
    String myTimestamp;
    String myGeography;
    List<Long> myArray;
    MyStruct myStruct;

    public static MyData fromTableRow(TableRow row) {
      MyData data = new MyData();

      data.myString = (String) row.get("string_field");
      data.myInt64 = Long.parseLong((String) row.get("int64_field"));
      data.myFloat64 = (Double) row.get("float64_field");
      data.myNumeric = new BigDecimal((String) row.get("numeric_field"));
      data.myBoolean = (Boolean) row.get("bool_field");
      data.myBytes = Base64.getDecoder().decode((String) row.get("bytes_field"));

      data.myDate = LocalDate.parse((String) row.get("date_field")).toString();
      data.myDateTime = LocalDateTime.parse((String) row.get("datetime_field")).toString();
      data.myTime = LocalTime.parse((String) row.get("time_field")).toString();
      data.myTimestamp =
          Instant.parse(
                  ((String) row.get("timestamp_field")).replace(" UTC", "Z").replace(" ", "T"))
              .toString();

      data.myGeography = (String) row.get("geography_field");

      data.myArray =
          ((List<Object>) row.get("array_field"))
              .stream()
                  .map(element -> Long.parseLong((String) element))
                  .collect(Collectors.toList());

      Map<String, Object> structValues = (Map<String, Object>) row.get("struct_field");
      data.myStruct = new MyStruct();
      data.myStruct.stringValue = (String) structValues.get("string_value");
      data.myStruct.int64Value = Long.parseLong((String) structValues.get("int64_value"));

      return data;
    }
  }
}
// [END bigquery_my_data]
