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

/*
thrift --gen java:private-members,fullcamel \
    -out sdks/java/io/thrift/src/test/java/ \
    sdks/java/io/thrift/src/test/thrift/payload.thrift

./gradlew :sdks:java:extensions:sql:spotlessApply
*/

namespace java org.apache.beam.sdk.io.thrift.payloads

struct TestThriftMessage {
  1: required i64 f_long
  2: required i32 f_int
  3: required double f_double
  4: required string f_string
  5: required list<double> f_double_array = 5
}

struct SimpleThriftMessage {
    1: required i32 id
    2: required string name
}

struct ItThriftMessage {
    1: required i64 f_long
    2: required i32 f_int
    3: required string f_string
}
