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
package org.apache.beam.sdk.io.snowflake;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;

/**
 * Custom DoFn that maps {@link Object[]} into CSV line to be saved to Snowflake.
 *
 * <p>Adds Snowflake-specific quotations around strings.
 */
class MapUserTypeToCsvLineFn<T> extends DoFn<T, String> {
  private final SerializableFunction<T, Object[]> csvJsonFormatFunction;
  private final String quotationMark;

  public MapUserTypeToCsvLineFn(
      SerializableFunction<T, Object[]> csvJsonFormatFunction, String quotationMark) {
    this.csvJsonFormatFunction = csvJsonFormatFunction;
    this.quotationMark = quotationMark;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    Object[] columns = this.csvJsonFormatFunction.apply(context.element());
    List<Object> csvItems = new ArrayList<>();
    for (Object o : columns) {
      if (o instanceof String) {
        String field = (String) o;
        field = field.replace("'", "''");
        field = quoteField(field);

        csvItems.add(field);
      } else {
        csvItems.add(o);
      }
    }
    context.output(Joiner.on(",").useForNull("").join(csvItems));
  }

  private String quoteField(String field) {
    return quoteField(field, this.quotationMark);
  }

  private String quoteField(String field, String quotation) {
    return String.format("%s%s%s", quotation, field, quotation);
  }
}
