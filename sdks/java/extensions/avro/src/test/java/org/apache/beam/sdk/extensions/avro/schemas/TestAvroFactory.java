// /*
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package org.apache.beam.sdk.extensions.avro.schemas;

// import java.lang.reflect.Constructor;
// import java.nio.ByteBuffer;
// import java.util.List;
// import java.util.Map;
// import org.apache.avro.Schema;
// import org.joda.time.DateTime;
// import org.joda.time.LocalDate;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// /** Create a {@link TestAvro} instance with different constructors. */
// public class TestAvroFactory {
//   private static final Logger LOG = LoggerFactory.getLogger(TestAvroFactory.class);
//   private static final String VERSION_AVRO = Schema.class.getPackage().getImplementationVersion();

//   public static TestAvro newInstance(
//       Boolean boolNonNullable,
//       Integer integer,
//       Long aLong,
//       Float aFloat,
//       Double aDouble,
//       CharSequence string,
//       ByteBuffer bytes,
//       fixed4 fixed,
//       LocalDate date,
//       DateTime timestampMillis,
//       TestEnum testEnum,
//       TestAvroNested row,
//       List<TestAvroNested> array,
//       Map<CharSequence, TestAvroNested> map) {
//     try {
//       if (VERSION_AVRO.equals("1.8.2")) {
//         Constructor<?> constructor =
//             TestAvro.class.getDeclaredConstructor(
//                 Boolean.class,
//                 Integer.class,
//                 Long.class,
//                 Float.class,
//                 Double.class,
//                 CharSequence.class,
//                 ByteBuffer.class,
//                 fixed4.class,
//                 org.joda.time.LocalDate.class,
//                 org.joda.time.DateTime.class,
//                 TestEnum.class,
//                 TestAvroNested.class,
//                 java.util.List.class,
//                 java.util.Map.class);

//         return (TestAvro)
//             constructor.newInstance(
//                 boolNonNullable,
//                 integer,
//                 aLong,
//                 aFloat,
//                 aDouble,
//                 string,
//                 bytes,
//                 fixed,
//                 date,
//                 timestampMillis,
//                 testEnum,
//                 row,
//                 array,
//                 map);
//       } else {
//         Constructor<?> constructor =
//             TestAvro.class.getDeclaredConstructor(
//                 Boolean.class,
//                 Integer.class,
//                 Long.class,
//                 Float.class,
//                 Double.class,
//                 CharSequence.class,
//                 ByteBuffer.class,
//                 fixed4.class,
//                 java.time.LocalDate.class,
//                 java.time.Instant.class,
//                 TestEnum.class,
//                 TestAvroNested.class,
//                 java.util.List.class,
//                 java.util.Map.class);

//         return (TestAvro)
//             constructor.newInstance(
//                 boolNonNullable,
//                 integer,
//                 aLong,
//                 aFloat,
//                 aDouble,
//                 string,
//                 bytes,
//                 fixed,
//                 java.time.LocalDate.of(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth()),
//                 java.time.Instant.ofEpochMilli(timestampMillis.getMillis()),
//                 testEnum,
//                 row,
//                 array,
//                 map);
//       }
//     } catch (ReflectiveOperationException e) {
//       LOG.error(String.format("Fail to create a TestAvro instance: %s", e.getMessage()));
//       return new TestAvro(); // return an empty instance to fail the tests
//     }
//   }
// }
