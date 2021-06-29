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
This thrift file is used to generate the TestThrift* classes:

thrift --gen java:beans \
  -out sdks/java/io/thrift/src/test/java/ \
  sdks/java/io/thrift/src/test/resources/thrift/thrift_test.thrift

./gradlew :sdks:java:io:thrift:spotlessApply
*/

namespace java org.apache.beam.sdk.io.thrift

typedef string Name
typedef i16 Age

struct TestThriftInnerStruct {
    1: Name testNameTypedef = "kid"
    2: Age testAgeTypedef = 12
}

enum TestThriftEnum { C1, C2 }

union TestThriftUnion {
    1: TestThriftInnerStruct snake_case_nested_struct;
    2: TestThriftEnum camelCaseEnum;
}

typedef set<string> StringSet

struct TestThriftStruct {
    1: i8 testByte
    2: i16 testShort
    3: i32 testInt
    4: i64 testLong
    5: double testDouble
    6: map<string, i16> stringIntMap
    7: binary testBinary
    8: bool testBool
    9: list<i32> testList
    10: StringSet testStringSetTypedef
    11: TestThriftEnum testEnum
    12: TestThriftInnerStruct testNested
    13: TestThriftUnion testUnion
}
