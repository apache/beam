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
package org.apache.beam.sdk.extensions.smb.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

/** Utilities for SMB unit tests. */
public class TestUtils {

  static final Schema SCHEMA =
      Schema.createRecord(
          "user",
          "",
          "org.apache.beam.sdk.extensions.smb",
          false,
          Lists.newArrayList(
              new Field("name", Schema.create(Type.STRING), "", null),
              new Field("age", Schema.create(Type.INT), "", null)));

  static final Coder<GenericRecord> USER_CODER = AvroCoder.of(SCHEMA);

  static GenericRecord createUserRecord(String name, int age) {
    GenericData.Record result = new GenericData.Record(SCHEMA);
    result.put("name", name);
    result.put("age", age);

    return result;
  }

  static AvroBucketMetadata<Integer> tryCreateMetadata(int numBuckets, HashType hashType) {
    try {
      return new AvroBucketMetadata<Integer>(numBuckets, Integer.class, hashType, "age");
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException();
    }
  }
}
