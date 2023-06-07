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
package org.apache.beam.sdk.extensions.avro.schemas;

import java.lang.reflect.Constructor;
import org.apache.avro.Schema;
import org.joda.time.LocalDate;

/** Create a {@link TestAvroConversion} instance with different constructors. */
public class TestAvroConversionFactory {

  private static final String VERSION_AVRO = Schema.class.getPackage().getImplementationVersion();

  public static TestAvroConversion newInstance(LocalDate date) throws Exception {
    if (VERSION_AVRO.equals("1.8.2")) {
      Constructor<?> constructor = TestAvroConversion.class.getDeclaredConstructor(LocalDate.class);
      return (TestAvroConversion) constructor.newInstance(date);
    } else {
      Constructor<?> constructor =
          TestAvroConversion.class.getDeclaredConstructor(java.time.LocalDate.class);
      return (TestAvroConversion)
          constructor.newInstance(
              java.time.LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth()));
    }
  }
}
